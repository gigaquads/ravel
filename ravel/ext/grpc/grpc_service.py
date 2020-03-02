import os
import json
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

from appyratus.schema import Schema, fields
from appyratus.memoize import memoized_property
from appyratus.utils import StringUtils, FuncUtils, DictUtils, DictObject

from ravel.util import is_resource, is_batch, get_class_name
from ravel.util.json_encoder import JsonEncoder
from ravel.util.loggers import console
from ravel.app.base import Application

from .grpc_function import GrpcFunction
from .grpc_client import GrpcClient
from .util import (
    touch_file,
    is_port_in_use,
    bind_message,
    dump_result_obj,
)


DEFAULT_HOST = '127.0.0.1'
DEFAULT_PORT = '50051'
DEFAULT_GRACE = 5
DEFAULT_IS_SECURE = False
REGISTRY_PROTO_FILE = 'app.proto'
PB2_MODULE_PATH_FSTR = '{}.grpc.app_pb2'
PB2_GRPC_MODULE_PATH_FSTR = '{}.grpc.app_pb2_grpc'
PROTOC_COMMAND_FSTR = '''
python3 -m grpc_tools.protoc
    -I {include_dir}
    --python_out={build_dir}
    --grpc_python_out={build_dir}
    {proto_file}
'''


class GrpcService(Application):
    """
    Grpc server and client interface.
    """

    class GrpcOptionsSchema(Schema):
        client_host = fields.String(required=True, default=DEFAULT_HOST)
        server_host = fields.String(required=True, default=DEFAULT_HOST)
        secure_channel = fields.String(required=True, default=DEFAULT_IS_SECURE)
        port = fields.String(required=True, default=DEFAULT_PORT)
        grace = fields.String(required=True, default=DEFAULT_GRACE)

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
    def action_type(self):
        return GrpcFunction

    def on_bootstrap(self, build=False, options: Dict = None):
        grpc = self.grpc

        # get the python package containing this Application
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

        # generate default values into gRPC options data
        schema = self.GrpcOptionsSchema()
        options, errors = schema.process(computed_options)
        if errors:
            raise Excpetion(f'invalid gRPC options: {errors}')

        grpc.options = DictObject(options)

        grpc.options.server_address = (
            f"{grpc.options['server_host']}:{grpc.options['port']}"
        )
        grpc.options.client_address = (
            f"{grpc.options['client_host']}:{grpc.options['port']}"
        )

        # create the build directory and add it to PYTHONPATH
        sys.path.append(grpc.build_dir)

        # build dotted paths to the auto-generated pb2, pb2_grpc modules
        pb2_mod_path = PB2_MODULE_PATH_FSTR.format(pkg_path)
        pb2_grpc_mod_path = PB2_GRPC_MODULE_PATH_FSTR.format(pkg_path)

        # try to import the pb2 module just to see if it exists
        try:
            import_module(pb2_mod_path, pkg_path)
            pb2_module_dne = False    # dne: does not exist
        except Exception:
            pb2_module_dne = True
            #traceback.print_exc()

        # build the pb2 and pb2_grpc modules
        if build or pb2_module_dne:
            self._build_pb2_modules()
            time.sleep(0.25)

        # now import the dynamically-generated pb2 modules
        self.grpc.pb2 = import_module(pb2_mod_path, pkg_path)
        self.grpc.pb2_grpc = import_module(pb2_grpc_mod_path, pkg_path)

        # build a lookup table of protobuf response Message types
        self.grpc.response_types = {
            action: getattr(
                self.grpc.pb2,
                f'{StringUtils.camel(get_class_name(action.schemas.response))}'
            )
            for action in self.actions.values()
        }

    def build(self, manifest=None, options: Dict = None):
        self.bootstrap(manifest=manifest, build=True, options=options)

    def on_decorate(self, action):
        pass

    def on_request(self, action, request, *args, **kwargs):
        """
        Take the attributes on the incoming protobuf Message object and
        map them to the args and kwargs expected by the action target.
        """
        console.debug(f'calling "{action.name}" RPC method')

        # get field data to process into args and kwargs
        all_arguments = {
            k: getattr(request, k, None)
            for k in action.schemas.request.fields
        }
        # TODO: revise on_request to take just the new Request object
        # TODO: inject the Request object into all_arguments

        # process field_data into args and kwargs
        args, kwargs = FuncUtils.partition_arguments(
            action.signature, all_arguments
        )
        return (args, kwargs)

    def on_response(self, action, result, *raw_args, **raw_kwargs):
        """
        Map the return dict from the action to the expected outgoing protobuf
        response Message object.
        """
        response_type = self.grpc.response_types[action]
        response = response_type()

        if result:
            schema = action.schemas.response
            dumped_result = dump_result_obj(result)
            response_data, errors = schema.process(dumped_result, strict=True)
            response = bind_message(response, response_data)
            if errors:
                console.error(
                    message=f'response validation errors',
                    data={'errors': errors}
                )
        return response

    def on_start(self):
        """
        Start the RPC client or server.
        """
        if is_port_in_use(self.grpc.options.server_address):
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
        console.info(
            message='gRPC server started. Press ctrl+c to stop.',
            data={
                'listen': self.grpc.options.server_address,
                'methods': list(self.api.keys()),
            }
        )

        stop_grace_period = 5  # seconds

        try:
            while True:
                time.sleep(9999)
        except KeyboardInterrupt:
            if self.grpc.server is not None:
                console.info(
                    message='stopping gRPC server',
                    data={'grace': stop_grace_period}
                )
                self.grpc.server.stop(grace=stop_grace_period)

    def _grpc_servicer_factory(self):
        """
        Import the abstract base Servicer class from the autogenerated pb2_grpc
        module and derive a new subclass that inherits this Application's action
        objects as its interface implementation.
        """
        servicer_type_name = 'GrpcApplicationServicer'
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
            servicer_type_name, (abstract_type, ), self.actions.copy()
        )()

        # register the Servicer with the server
        servicer.add_to_grpc_server = (
            self.grpc.pb2_grpc.add_GrpcApplicationServicer_to_server
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

        dot_file_name  = os.path.join(self.grpc.build_dir, '.ravel')
        if not os.path.isfile(dot_file_name):
            with open(dot_file_name, 'w') as dot_file:
                json.dump({'scanner': {'ignore': True}}, dot_file)
        else:
            with open(dot_file_name, 'rw') as dot_file:
                dot_data = json.load(dot_file) or {}
                dot_data.setdefault('scanner', {})['ignore'] = True
                json.dump(dot_data, dot_file)

        # generate the .proto file and generate grpc python modules from it
        self._grpc_generate_proto_file()
        self._grpc_compile_pb2_modules()

    def _grpc_generate_proto_file(self):
        """
        Iterate over function actions, using request and response schemas to
        generate protobuf message and service types.
        """
        visited_schema_types = set()
        lines = ['syntax = "proto3";']
        func_decls = []

        for action in self.actions.values():
            # generate protocol buffer Message types from action schemas
            if action.schemas.request:
                type_name = get_class_name(action.schemas.request)
                if type_name not in visited_schema_types:
                    lines.append(action.generate_request_message_type())
                    visited_schema_types.add(type_name)
            if action.schemas.response:
                type_name = get_class_name(action.schemas.response)
                if type_name not in visited_schema_types:
                    lines.append(action.generate_response_message_type())
                    visited_schema_types.add(type_name)

            # function declaration MUST be generated AFTER the message types
            func_decls.append(action.generate_protobuf_function_declaration())

        lines.append('service GrpcApplication {')

        for decl in func_decls:
            lines.append('  ' + decl)

        lines.append('}\n')

        source = '\n'.join(lines)

        console.info(
            message='generating gRPC proto file',
            data={'destination': self.grpc.proto_file}
        )

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
        protoc_command = PROTOC_COMMAND_FSTR.format(
            include_dir=os.path.realpath(
                os.path.dirname(self.grpc.proto_file)
            ) or '.',
            build_dir=self.grpc.build_dir,
            proto_file=os.path.basename(self.grpc.proto_file),
        ).strip()

        console.info(
            message='generating gRPC pb2 modules',
            data={'command': protoc_command.split('\n')}
        )

        err_msg = subprocess.getoutput(
            re.sub(r'\s+', ' ', protoc_command)
        )
        if err_msg:
            exit(err_msg)
