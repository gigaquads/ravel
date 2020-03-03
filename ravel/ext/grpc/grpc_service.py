import os
import json
import sys
import inspect
import shutil
import subprocess
import traceback
import time
import re
import multiprocessing as mp
import socket
import pickle
import codecs

import grpc

from threading import get_ident
from concurrent.futures import ThreadPoolExecutor
from typing import Text, List, Dict, Type, Union, Type
from importlib import import_module

from google.protobuf.message import Message
from appyratus.schema import Schema, fields
from appyratus.utils import StringUtils, FuncUtils, DictUtils, DictObject

from ravel.util import is_resource, is_batch, get_class_name, is_port_in_use
from ravel.util.json_encoder import JsonEncoder
from ravel.util.loggers import console
from ravel.app.base import Application

from .grpc_method import GrpcMethod
from .grpc_client import GrpcClient
from .constants import (
    DEFAULT_HOST,
    DEFAULT_PORT,
    DEFAULT_GRACE,
    DEFAULT_IS_SECURE,
    REGISTRY_PROTO_FILE,
    PB2_MODULE_PATH_FSTR,
    PB2_GRPC_MODULE_PATH_FSTR,
    PROTOC_COMMAND_FSTR,
)


class GrpcService(Application):
    """
    Grpc server.

    All configuration options and data structures pertaining to gRPC itself are
    kept in self.grpc. For the most part, this object is constructed during
    bootstrap.

    It is possible to generate a gRPC client for the service via the "client"
    property. The service must be bootstrapped before accessing the client.

    Note that gRPC manages its server as a thread pool. When we call
    GrpcService.start, we are really just starting the threads in the pool and
    then putting the main thread in a spin lock.
    """

    class GrpcOptionsSchema(Schema):
        """
        Manifest gRPC options schema
        """
        client_host = fields.String(required=True, default=DEFAULT_HOST)
        server_host = fields.String(required=True, default=DEFAULT_HOST)
        secure_channel = fields.Bool(required=True, default=DEFAULT_IS_SECURE)
        port = fields.String(required=True, default=DEFAULT_PORT)
        grace = fields.Float(required=True, default=DEFAULT_GRACE)


    def __init__(self, middleware=None, *args, **kwargs):
        super().__init__(middleware=middleware, *args, **kwargs)
        self.grpc = DictObject()
        self.grpc.options = DictObject()
        self.grpc.max_workers = mp.cpu_count() + 1
        self.grpc.pb2 = None
        self.grpc.pb2_grpc = None
        self.grpc.server = None
        self.grpc.servicer = None
        self._client = None

    @property
    def client(self) -> GrpcClient:
        """
        Return a memoized GrpcClient instance. However, if the service isn't
        bootstrapped, None will be returned, as the client derives itself from a
        bootstrapped service.
        """
        if not self.is_bootstrapped:
            return None
        elif self._client is None:
            self._client = GrpcClient(self)
        return self._client

    @property
    def action_type(self):
        return GrpcMethod

    def on_bootstrap(self, options: Dict = None, build=False):
        """
        Compute gRPC configuration variables, build the proto file and grpcio
        "pb2" Python modules, and compute other metadata needed to route
        requests to RPC API methods.
        """
        self._on_bootstrap_configure_grpc(options)
        self._on_bootstrap_build_and_import_grpc_modules(build)
        self._on_bootstrap_aggregate_response_message_types()

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

        # add a dummy request object so that partition_arguments can
        # run below; then remove it from the resulting args tuple.
        all_arguments['request'] = None

        # process field_data into args and kwargs
        args, kwargs = FuncUtils.partition_arguments(
            action.signature, all_arguments
        )

        args = args[1:]

        return (args, kwargs)

    def on_response(self, action, result, *raw_args, **raw_kwargs):
        """
        Map the return dict from the action to the expected outgoing protobuf
        response Message object.
        """
        response_type = self.grpc.response_types[action]
        response = response_type()
        response_data = None

        def validate(data):
            data, errors = schema.process(data, strict=True)
            if errors:
                console.error(
                    message=f'response validation errors',
                    data={'errors': errors}
                )
            return data


        if result:
            schema = action.schemas.response
            dumped_result = self._dump_result_obj(result)
            if isinstance(dumped_result, list):
                response_data = [validate(x) for x in dumped_result]
            else:
                response_data = validate(dumped_result)

        if response_data:
            if isinstance(response_data, list):
                response = iter(
                    self.build_response_message(response_type(), data)
                    for data in response_data
                )
            else:
                response = self.build_response_message(response, response_data)

        return response

    @staticmethod
    def on_start_worker(app):
        """
        This method runs as the "initializer" for each thread in the grpc
        server's thread pool. Override in subclass.
        """
        console.info(
            f're-boostrapping stores in worker thread {get_ident()}'
        )
        app.storage.bootstrap()

    def on_start(self):
        """
        Start the RPC client or server.
        """
        if is_port_in_use(self.grpc.options.server_address):
            console.error(f'{self.grpc.options.server_address} already in use')
            exit(-1)

        console.debug(
            f'creating gRPC thread pool with {self.grpc.max_workers} threads'
        )

        # the grpc server runs in a thread pool
        self.grpc.executor = ThreadPoolExecutor(
            max_workers=self.grpc.max_workers,
            initializer=self.on_start_worker,
            initargs=(self, )
        )

        for i in range(self.grpc.max_workers):
            self.grpc.executor.submit(lambda: None)

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
                'address': self.grpc.options.server_address,
                'methods': list(self.api.keys()),
            }
        )

        # spin-lock the main thread to keep the server running,
        # as the server runs as a daemon.
        try:
            while True:
                time.sleep(9999)
        except KeyboardInterrupt:
            # stop server on ctrl+c
            if self.grpc.server is not None:
                console.info(
                    message='stopping gRPC server',
                    data={'grace_period': self.grpc.options.grace}
                )
                self.grpc.server.stop(grace=self.grpc.options.grace)

    def _on_bootstrap_configure_grpc(self, options: Dict):
        """
        Compute the gRPC options dict and other values needed to run this
        application as a gRPC service.
        """
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

        port = options['port']
        server_host = options['server_host']
        client_host = options['client_host']

        grpc.options = DictObject(options)
        grpc.options.server_address = (f'{server_host}:{port}')
        grpc.options.client_address = (f'{client_host}:{port}')
        grpc.pb2_mod_path = PB2_MODULE_PATH_FSTR.format(pkg_path)
        grpc.pb2_grpc_mod_path = PB2_GRPC_MODULE_PATH_FSTR.format(pkg_path)

        # add build directory to PYTHONPATH; otherwise, gRPC won't know where
        # the pb2 modules are when it tries to import them.
        sys.path.append(grpc.build_dir)

    def _on_bootstrap_build_and_import_grpc_modules(self, build: bool):
        """
        Call an external grpcio command to generate the gRPC python modules
        necessary for running this service. Then we import them, because we need
        to derive client and servicer subclasses and procedures from the
        autogenerated base components contained therein.
        """
        grpc = self.grpc
        pkg_path = self.manifest.package

        # try to import the pb2 module just to see if it exists
        try:
            import_module(grpc.pb2_mod_path, pkg_path)
            pb2_module_dne = False    # dne: does not exist
        except Exception:
            console.warning(f'{grpc.pb2_mod_path} does not exist. rebuilding')
            pb2_module_dne = True

        # build the pb2 and pb2_grpc modules
        if build or pb2_module_dne:
            self._build_pb2_modules()
            time.sleep(0.25)

        # now import the dynamically-generated pb2 modules
        grpc.pb2 = import_module(grpc.pb2_mod_path, pkg_path)
        grpc.pb2_grpc = import_module(grpc.pb2_grpc_mod_path, pkg_path)

    def _on_bootstrap_aggregate_response_message_types(self):
        """
        Build a lookup table of protobuf response Message types for use when
        routing requests to their downstream actions.
        """
        grpc = self.grpc
        grpc.response_types = {
            action: getattr(
                grpc.pb2,
                f'{StringUtils.camel(get_class_name(action.schemas.response))}'
            )
            for action in self.actions.values()
        }

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
        servicer = type(servicer_type_name, (abstract_type, ), self.actions)()

        # register the Servicer with the server
        servicer.add_to_grpc_server = (
            self.grpc.pb2_grpc.add_GrpcApplicationServicer_to_server
        )

        return servicer

    def _build_pb2_modules(self):
        """
        Create the build dir inside the host package, generating the proto file
        and grpcio modules into it.
        """
        # recreate the build directory
        if os.path.isdir(self.grpc.build_dir):
            shutil.rmtree(self.grpc.build_dir)

        os.makedirs(os.path.join(self.grpc.build_dir), exist_ok=True)

        # touch the __init__.py file
        with open(os.path.join(self.grpc.build_dir, '__init__.py'), 'a'):
            pass

        # upsert the .ravel file in the build dir with a directive to ignore
        # this directory during project component discovery.
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
        self._generate_proto_file()
        self._compile_pb2_modules()

    def _generate_proto_file(self):
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

        for decl in func_decls: lines.append('  ' + decl)

        lines.append('}\n')

        source = '\n'.join(lines)

        console.info(
            message='generating gRPC proto file',
            data={'destination': self.grpc.proto_file}
        )

        with open(self.grpc.proto_file, 'w') as fout:
            fout.write(source)

    def _compile_pb2_modules(self):
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

    def build_response_message(self, message: Message, source: Dict) -> Message:
        """
        Recursively bind each value in a source dict to the corresponding
        attribute in a grpc Message object, resulting in the prepared response
        message, ready to be sent back to the client.
        """
        if source is None:
            return None

        for k, v in source.items():
            if isinstance(getattr(message, k), Message):
                sub_message = getattr(message, k)
                assert isinstance(v, dict)
                self.build_response_message(sub_message, v)
            elif isinstance(v, dict):
                v_bytes = codecs.encode(pickle.dumps(v), 'base64')
                setattr(message, k, v_bytes)
            elif isinstance(v, (list, tuple, set)):
                list_field = getattr(message, k)
                sub_message_type_name = (
                    '{}Schema'.format(k.title().replace('_', ''))
                )
                sub_message_type = getattr(message, sub_message_type_name, None)
                if sub_message_type:
                    list_field.extend(
                        self.build_response_message(sub_message_type(), v_i)
                        for v_i in v
                    )
                else:
                    v_prime = [
                        x if not isinstance(x, dict)
                            else codecs.encode(pickle.dumps(x), 'base64')
                        for x in v
                    ]
                    list_field.extend(v_prime)
            else:
                setattr(message, k, v)

        return message

    def _dump_result_obj(self, obj) -> Union[Dict, List]:
        """
        Currently, GrpcService is intended to be used for internal
        service-to-service comm. This is why we "dump" Resources here by simply
        returning their internal state dicts.
        """
        if is_resource(obj):
            return obj.internal.state
        elif is_batch(obj):
            return [x.internal.state for x in obj]
        elif isinstance(obj, (list, set, tuple)):
            return [self._dump_result_obj(x) for x in obj]
        elif isinstance(obj, dict):
            return {k: self._dump_result_obj(v) for k, v in obj.items()}
        else:
            return obj

