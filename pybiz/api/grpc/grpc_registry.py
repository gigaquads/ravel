import os
import sys
import socket
import inspect
import subprocess
import traceback
import time
import re
import pickle
import codecs

import grpc

from concurrent.futures import ThreadPoolExecutor
from typing import Text, List, Dict, Type
from importlib import import_module

from google.protobuf.message import Message

from appyratus.schema import fields
from appyratus.memoize import memoized_property
from appyratus.utils import StringUtils, FuncUtils
from appyratus.json import JsonEncoder

from pybiz.util import is_bizobj

from ..registry import Registry, RegistryProxy
from .grpc_registry_proxy import GrpcRegistryProxy
from .grpc_client import GrpcClient


class GrpcRegistry(Registry):
    """
    Grpc server and client interface.
    """

    def __init__(self, middleware=None):
        super().__init__(middleware=middleware)
        self._json_encoder = JsonEncoder()
        self._grpc_server = None
        self._grpc_servicer = None


    def bootstrap(self, manifest_filepath: Text, build_grpc=False):
        self.manifest.load(path=manifest_filepath)

        pkg_path = self.manifest.package
        pkg = import_module(pkg_path)
        pkg_dir = os.path.dirname(pkg.__file__)
        pb2_mod_path = '{}.grpc.registry_pb2'.format(pkg_path)
        pb2_grpc_mod_path = '{}.grpc.registry_pb2_grpc'.format(pkg_path)
        grpc_build_dir = os.path.join(pkg_dir, 'grpc')
        grpc_options = self.manifest.data.get('grpc', {})
        client_host = grpc_options.get('host', 'localhost')
        server_host = grpc_options.get('server_host', client_host)
        port = str(grpc_options.get('port', '50051'))

        self._grpc_server_addr = '{}:{}'.format(server_host, port)
        self._grpc_client_addr = '{}:{}'.format(client_host, port)
        self._protobuf_filepath = os.path.join(
            grpc_build_dir, 'registry.proto'
        )

        os.makedirs(os.path.join(grpc_build_dir), exist_ok=True)
        sys.path.append(grpc_build_dir)

        def on_error(name):
            if issubclass(sys.exc_info()[0], ImportError):
                traceback.print_exc()

        self.manifest.process(on_error=on_error)

        def touch(filepath):
            with open(os.path.join(filepath), 'a'):
                pass

        if build_grpc:
            sys.path.append(grpc_build_dir)
            touch(os.path.join(grpc_build_dir, '__init__.py'))
            self.grpc_build(dest=grpc_build_dir)

        self.pb2 = import_module(pb2_mod_path, pkg_path)
        self.pb2_grpc = import_module(pb2_grpc_mod_path, pkg_path)
        self._is_bootstrapped = True

    @property
    def proxy_type(self):
        return GrpcRegistryProxy

    @memoized_property
    def client(self) -> GrpcClient:
        return GrpcClient(self)

    @property
    def server_addr(self):
        return self._grpc_server_addr

    @property
    def client_addr(self):
        return self._grpc_client_addr

    def on_decorate(self, proxy):
        pass

    def on_request(self, proxy, signature, grpc_request):
        """
        Extract command line arguments and bind them to the arguments expected
        by the registered function's signature.
        """
        # TODO: Implement verbosity levels
        print('>>> Calling "{}" RPC function...'.format(proxy.name))
        arguments = {
            k: getattr(grpc_request, k, None)
            for k in proxy.request_schema.fields
        }
        args, kwargs = FuncUtils.partition_arguments(signature, arguments)
        return (args, kwargs)

    def on_response(self, proxy, result, *raw_args, **raw_kwargs):
        def recurseively_bind(target, data):
            if data is None:
                return None
            for k, v in data.items():
                if isinstance(v, Message):
                    recurseively_bind(getattr(target, k), v)
                else:
                    try:
                        setattr(target, k, v)
                    except Exception as exc:
                        print('Unable to bind "{}", type mismatch'.format(k))
                        raise exc
            return target

        # bind the returned dict values to the response protobuf message
        response_type = getattr(
            self.pb2, '{}Response'.format(StringUtils.camel(proxy.name))
        )
        resp = response_type()

        def to_dict(field, value):
            if is_bizobj(value):
                return value.dump()
            elif isinstance(field, fields.List):
                return [r.dump() if is_bizobj(r) else r for r in value]
            else:
                return value

        if result:
            dumped_result, dumped_error = proxy.response_schema.process(
                result, strict=True, pre_process=to_dict
            )
            for k, v in dumped_result.items():
                field = proxy.response_schema.fields[k]
                if isinstance(field, fields.Dict):
                    v_bytes = codecs.encode(pickle.dumps(v), 'base64')
                    setattr(resp, k, v_bytes)
                elif isinstance(field, fields.List):
                    nested_resp_type = getattr(resp, field.nested.__class__.__name__)
                    getattr(resp, k).extend([recurseively_bind(nested_resp_type(), _v) for _v in v])
                elif isinstance(getattr(resp, k), Message):
                    recurseively_bind(getattr(resp, k), v)
                else:
                    setattr(resp, k, v)
        return resp

    def start(self, initializer=None, grace=2):
        """
        Start the RPC client or server.
        """
        if self._is_port_in_use():
            print(
                '>>> {} already in use. '
                'Exiting...'.format(self._grpc_server_addr)
            )
            exit(-1)

        executor = ThreadPoolExecutor(
            max_workers=None,  # TODO: put this value in manifest
            # XXX TypeError: __init__() got an unexpected keyword argument 'initializer'
            #initializer=initializer,
        )

        # build the grpc server
        self._grpc_server = grpc.server(executor)
        self._grpc_server.add_insecure_port(self.server_addr)

        # build the grpc servicer and connect with server
        self._grpc_servicer = self._grpc_servicer_factory()
        self._grpc_servicer.add_to_grpc_server(
            self._grpc_servicer, self._grpc_server
        )

        # start the server. note that it is non-blocking.
        self._grpc_server.start()

        # enter spin lock
        print('>>> gRPC server is running. Press ctrl+c to stop.')
        print('>>> Listening on {}...'.format(self._grpc_server_addr))
        try:
            while True:
                time.sleep(32)
        except KeyboardInterrupt:
            print()
            if self._grpc_server is not None:
                print('>>> Stopping grpc server...')
                self._grpc_server.stop(grace=grace)
            print('>>> Groodbye!')

    def _is_port_in_use(self):
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            host, port_str = self._grpc_client_addr.split(':')
            sock.bind((host, int(port_str)))
            return False
        except OSError as err:
            if err.errno == 48:
                return True
            else:
                raise err
        finally:
            sock.close()

    def _grpc_servicer_factory(self):
        servicer_type_name = 'GrpcRegistryServicer'
        abstract_type = None

        for k, v in inspect.getmembers(self.pb2_grpc):
            if k == servicer_type_name:
                abstract_type = v
                break

        if abstract_type is None:
            raise Exception('could not find grpc Servicer class')

        methods = {p.name: p for p in self.proxies}
        servicer_type = type(servicer_type_name, (abstract_type, ), methods)
        servicer = servicer_type()
        servicer.add_to_grpc_server = (
            self.pb2_grpc.add_GrpcRegistryServicer_to_server
        )
        return servicer

    def grpc_build(self, dest):
        self._grpc_generate_proto_file(dest)
        self._grpc_compile_proto_file(dest)

    def _grpc_generate_proto_file(self, dest):
        """
        Iterate over function proxies, using request and response schemas to
        generate protobuf message and service types.
        """
        chunks = ['syntax = "proto3";']
        func_decls = []
        for proxy in self.proxies:
            chunks.extend(proxy.generate_protobuf_message_types())
            func_decls.append(proxy.generate_protobuf_function_declaration())

        chunks.append('service GrpcRegistry {')
        for decl in func_decls:
            chunks.append('  ' + decl)
        chunks.append('}\n')

        source = '\n'.join(chunks)

        if self._protobuf_filepath:
            with open(self._protobuf_filepath, 'w') as fout:
                fout.write(source)

        return source

    def _grpc_compile_proto_file(self, dest):
        """
        Compile the protobuf file resulting from .
        """
        include_dir = os.path.realpath(
            os.path.dirname(self._protobuf_filepath)
        )
        proto_file = os.path.basename(self._protobuf_filepath)

        cmd = re.sub(
            r'\s+', ' ', '''
            python3 -m grpc_tools.protoc
                -I {include_dir}
                --python_out={build_dir}
                --grpc_python_out={build_dir}
                {proto_file}
        '''.format(
                include_dir=include_dir or '.',
                build_dir=dest,
                proto_file=proto_file
            ).strip()
        )

        print(cmd)

        output = subprocess.getoutput(cmd)
        if output:
            print(output)
