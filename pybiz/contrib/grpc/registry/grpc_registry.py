import os
import sys
import socket
import inspect
import shutil
import subprocess
import traceback
import time
import re
import pickle
import codecs
import multiprocessing as mp

import grpc

from concurrent.futures import ThreadPoolExecutor
from typing import Text, List, Dict, Type
from importlib import import_module

from google.protobuf.message import Message

from appyratus.schema import fields
from appyratus.memoize import memoized_property
from appyratus.utils import StringUtils, FuncUtils, DictUtils, DictObject
from appyratus.json import JsonEncoder

from pybiz.util import is_bizobj, is_bizlist, JsonEncoder
from pybiz.api.registry import Registry

from .grpc_registry_proxy import GrpcRegistryProxy
from ..grpc_client import GrpcClient

DEFAULT_HOST = '127.0.0.1'
DEFAULT_PORT = '50051'
DEFAULT_IS_SECURE = False
REGISTRY_PROTO_FILE = 'registry.proto'
PB2_MODULE_PATH_FSTR = '{}.grpc.registry_pb2'
PB2_GRPC_MODULE_PATH_FSTR = '{}.grpc.registry_pb2_grpc'


class GrpcRegistry(Registry):
    """
    Grpc server and client interface.
    """

    def __init__(self, middleware=None, initializer=None):
        super().__init__(middleware=middleware)
        self.grpc = DictObject()
        self.grpc.options = DictObject()
        self.grpc.pb2 = None
        self.grpc.pb2_grpc = None
        self.grpc.server = None
        self.grpc.servicer = None
        self.initializer = initializer

    @memoized_property
    def client(self) -> GrpcClient:
        return GrpcClient(self)

    @property
    def proxy_type(self):
        return GrpcRegistryProxy

    def on_bootstrap(self, rebuild=False, options: Dict = None):
        grpc = self.grpc

        # get the python package containing this Registry
        pkg_path = self.manifest.package
        pkg = import_module(self.manifest.package)

        # compute some file and module paths
        grpc.pkg_dir = os.path.dirname(pkg.__file__)
        grpc.build_dir = os.path.join(grpc.pkg_dir, 'grpc')
        grpc.proto_file = os.path.join(grpc.build_dir, REGISTRY_PROTO_FILE)

        # compute grpc options dict from options kwarg and manifest data
        kwarg_options = options or {}
        manifest_options = self.manifest.data.get('grpc', {})
        computed_options = DictUtils.merge(kwarg_options, manifest_options)

        grpc.options = DictObject(computed_options)
        grpc.options.data.setdefault('client_host', DEFAULT_HOST)
        grpc.options.data.setdefault('server_host', DEFAULT_HOST)
        grpc.options.data.setdefault('secure_channel', DEFAULT_IS_SECURE)
        grpc.options.data.setdefault('port', DEFAULT_PORT)

        grpc.options.server_address = (
            f'{grpc.options.server_host}:{grpc.options.port}'
        )
        grpc.options.client_address = (
            f'{grpc.options.client_host}:{grpc.options.port}'
        )

        # create the build directory and add it to PYTHONPATH
        sys.path.append(grpc.build_dir)

        # build dotted paths to the auto-generated pb2, pb2_grpc modules
        pb2_mod_path = PB2_MODULE_PATH_FSTR.format(pkg_path)
        pb2_grpc_mod_path = PB2_GRPC_MODULE_PATH_FSTR.format(pkg_path)

        # try to import the pb2 module just to see if it exists
        try:
            import_module(pb2_mod_path, pkg_path)
            pb2_module_dne = False  # dne: does not exist
        except Exception:
            pb2_module_dne = True
            #traceback.print_exc()

        # build the pb2 and pb2_grpc modules
        if rebuild or pb2_module_dne:
            self._build_pb2_modules()
            time.sleep(0.25)

        # now import the dynamically-generated pb2 modules
        self.grpc.pb2 = import_module(pb2_mod_path, pkg_path)
        self.grpc.pb2_grpc = import_module(pb2_grpc_mod_path, pkg_path)

        # build a lookup table of protobuf response Message types
        self.grpc.response_types = {
            proxy: getattr(
                self.grpc.pb2, f'{StringUtils.camel(proxy.name)}Response'
            )
            for proxy in self.proxies.values()
        }

    def on_decorate(self, proxy):
        pass

    def on_request(self, proxy, request, *args, **kwargs):
        """
        Take the attributes on the incoming protobuf Message object and
        map them to the args and kwargs expected by the proxy target.
        """
        print(f'>>> Calling "{proxy.name}" RPC function...')

        # get field data to process into args and kwargs
        all_arguments = {
            k: getattr(request, k, None)
            for k in proxy.request_schema.fields
        }

        # process field_data into args and kwargs
        args, kwargs = FuncUtils.partition_arguments(
            proxy.signature, all_arguments
        )
        return (args, kwargs)

    def on_response(self, proxy, result, *raw_args, **raw_kwargs):
        """
        Map the return dict from the proxy to the expected outgoing protobuf
        response Message object.
        """
        response_type = self.grpc.response_types[proxy]
        response = response_type()

        if result:
            schema = proxy.response_schema
            dumped_result = _dump_result_obj(result)
            response_data, errors = schema.process(dumped_result, strict=True)
            response = _bind_message(response, response_data)
            if errors:
                print(f'>>> response validation errors: {errors}')

        return response

    def on_start(self):
        """
        Start the RPC client or server.
        """
        if _is_port_in_use(self.grpc.options.server_address):
            exit(
                f'>>> {self.grpc.options.server_address} already in use! '
                f'Exiting...'
            )

        # the grpc server runs in a thread pool
        self.grpc.executor = ThreadPoolExecutor(
            max_workers=mp.cpu_count() + 1,
            initializer=self.initializer,
        )
        # build the grpc server
        self.grpc.server = grpc.server(self.grpc.executor)
        self.grpc.server.add_insecure_port(self.grpc.options.server_address)

        # build the grpc servicer and connect with server
        self.grpc.servicer = self._grpc_servicer_factory()
        self.grpc.servicer.add_to_grpc_server(
            self.grpc.servicer, self.grpc.server
        )

        # start the server. note that it is non-blocking.
        self.grpc.server.start()

        # now suspend the main thread while the grpc server runs
        # in the ThreadPoolExecutor.
        print('>>> gRPC server is running. Press ctrl+c to stop.')
        print(f'>>> Listening on {self.grpc.options.server_address}...')

        try:
            while True:
                time.sleep(9999)
        except KeyboardInterrupt:
            print()
            if self.grpc.server is not None:
                print('>>> Stopping grpc server...')
                self.grpc.server.stop(grace=5)
            print('>>> Groodbye!')

    def _grpc_servicer_factory(self):
        """
        Import the abstract base Servicer class from the autogenerated pb2_grpc
        module and derive a new subclass that inherits this Registry's proxy
        objects as its interface implementation.
        """
        servicer_type_name = 'GrpcRegistryServicer'
        abstract_type = None

        # get a reference to the grpc abstract base Servicer  class
        for k, v in inspect.getmembers(self.grpc.pb2_grpc):
            if k == servicer_type_name:
                abstract_type = v
                break
        if abstract_type is None:
            raise Exception('could not find grpc Servicer class')

        # create dynamic Servicer subclass
        servicer = type(
            servicer_type_name, (abstract_type, ), self.proxies.copy()
        )()

        # register the Servicer with the server
        servicer.add_to_grpc_server = (
            self.grpc.pb2_grpc.add_GrpcRegistryServicer_to_server
        )

        return servicer

    def _build_pb2_modules(self):
        # recreate the build directory
        if os.path.isdir(self.grpc.build_dir):
            shutil.rmtree(self.grpc.build_dir)
        os.makedirs(os.path.join(self.grpc.build_dir), exist_ok=True)

        # touch the __init__.py file
        with open(os.path.join(self.grpc.build_dir, '__init__.py'), 'a'):
            pass

        # generate the .proto file and generate grpc python modules from it
        self._grpc_generate_proto_file()
        self._grpc_compile_pb2_modules()

    def _grpc_generate_proto_file(self):
        """
        Iterate over function proxies, using request and response schemas to
        generate protobuf message and service types.
        """
        lines = ['syntax = "proto3";']

        func_decls = []
        for proxy in self.proxies.values():
            lines.extend(proxy.generate_protobuf_message_types())
            func_decls.append(proxy.generate_protobuf_function_declaration())


        lines.append('service GrpcRegistry {')

        for decl in func_decls:
            lines.append('  ' + decl)

        lines.append('}\n')

        source = '\n'.join(lines)

        if self.grpc.proto_file:
            with open(self.grpc.proto_file, 'w') as fout:
                fout.write(source)

        return source

    def _grpc_compile_pb2_modules(self):
        """
        Compile the grpc .proto file, generating pb2 and pb2_grpc modules in the
        build directory. These modules contain abstract base classes required
        by the grpc server and client.
        """
        # build the shell command to run....
        protoc_command = re.sub(
            r'\s+', ' ', '''
            python3 -m grpc_tools.protoc
                -I {include_dir}
                --python_out={build_dir}
                --grpc_python_out={build_dir}
                {proto_file}
        '''.format(
                include_dir=os.path.realpath(
                    os.path.dirname(self.grpc.proto_file)
                ) or '.',
                build_dir=self.grpc.build_dir,
                proto_file=os.path.basename(self.grpc.proto_file),
            ).strip()
        )
        print(protoc_command + '\n')
        err_msg = subprocess.getoutput(protoc_command)
        if err_msg:
            exit(err_msg)


def _touch_file(filepath):
    """
    Ensure a file exists at the given file path, creating one if does not
    exist.
    """
    with open(os.path.join(filepath), 'a'):
        pass


def _is_port_in_use(addr):
    """
    Utility method for determining if the server address is already in use.
    """
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        host, port_str = addr.split(':')
        sock.bind((host, int(port_str)))
        return False
    except OSError as err:
        if err.errno == 48:
            return True
        else:
            raise err
    finally:
        sock.close()


def _bind_message(message, source: Dict):
    if source is None:
        return None
    for k, v in source.items():
        if isinstance(getattr(message, k), Message):
            sub_message = getattr(message, k)
            assert isinstance(v, dict)
            _bind_message(sub_message, v)
        elif isinstance(v, dict):
            v_bytes = codecs.encode(pickle.dumps(v), 'base64')
            setattr(message, k, v_bytes)
        elif isinstance(v, (list, tuple, set)):
            list_field = getattr(message, k)
            sub_message_type_name = (
                '{}Schema'.format(k.title().replace('_', ''))
            )
            sub_message = getattr(message, sub_message_type_name, None)
            if sub_message:
                list_field.extend(
                    _bind_message(sub_message(), v_i)
                    for v_i in v
                )
            else:
                list_field.extend(v)
        else:
            setattr(message, k, v)
    return message


def _dump_result_obj(obj):
    if is_bizobj(obj) or is_bizlist(obj):
        return obj.dump(raw=True)
    elif isinstance(obj, (list, set, tuple)):
        return [_dump_result_obj(x) for x in obj]
    elif isinstance(obj, dict):
        return {k: _dump_result_obj(v) for k, v in obj.items()}
    else:
        return obj
