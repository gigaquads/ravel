import os
import sys
import traceback
import importlib
import inspect
import subprocess
import time
import re

import grpc

from concurrent.futures import ThreadPoolExecutor
from typing import Text, List
from importlib import import_module

from appyratus.validation.schema import Schema
from appyratus.decorators import memoized_property
from appyratus.util import TextTransform, FuncUtils

from ..base import FunctionRegistry, FunctionDecorator, FunctionProxy
from .grpc_client import GrpcClient
from .grpc_remote_dao import remote_dao_endpoint_factory


class GrpcFunctionRegistry(FunctionRegistry):
    """
    Grpc server and client interface.
    """

    def __init__(self):
        super().__init__()
        self._grpc_server = None
        self._grpc_servicer = None

    def bootstrap(self, manifest_filepath: Text, build_grpc=False):
        super().bootstrap(manifest_filepath, defer_processing=True)
        pkg_path = self.manifest.package
        pkg = importlib.import_module(pkg_path)
        pkg_dir = os.path.dirname(pkg.__file__)
        pb2_mod_path = '{}._grpc.registry_pb2'.format(pkg_path)
        pb2_grpc_mod_path = '{}._grpc.registry_pb2_grpc'.format(pkg_path)
        grpc_build_dir = os.path.join(pkg_dir, '_grpc')
        grpc_options = self.manifest.data.get('grpc', {})
        server_host = grpc_options.get('server-host', '::')
        client_host = grpc_options.get('client-host', 'localhost')
        port = grpc_options.get('port', '50051')

        self._grpc_server_addr = '[{}]:{}'.format(server_host, port)
        self._grpc_client_addr = '{}:{}'.format(client_host, port)
        self._protobuf_filepath = os.path.join(pkg_dir, 'registry.proto')

        sys.path.append(grpc_build_dir)

        def onerror(name):
            if issubclass(sys.exc_info()[0], ImportError):
                breakpoint()

        # dynamically create an RPC endpoint (GrpcFunctionProxy) that defines
        # the remote Dao interface used by gRPC clients. Must be done before
        # the manifest.process method is called.
        self._remote_dao_endpoint = remote_dao_endpoint_factory(self)

        self.manifest.scanner.onerror = onerror
        self.manifest.process()

        def touch(filepath):
            with open(os.path.join(filepath), 'a'):
                pass

        if build_grpc:
            sys.path.append(grpc_build_dir)
            os.makedirs(os.path.join(grpc_build_dir), exist_ok=True)
            touch(os.path.join(grpc_build_dir, '__init__.py'))
            self.grpc_build(dest=grpc_build_dir)

        self.pb2 = import_module(pb2_mod_path, pkg_path)
        self.pb2_grpc = import_module(pb2_grpc_mod_path, pkg_path)

    @property
    def function_proxy_type(self):
        return GrpcFunctionProxy

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
        arguments = {
            k: getattr(grpc_request, k, None)
            for k in proxy.request_schema.fields
        }
        args, kwargs = FuncUtils.partition_arguments(signature, arguments)
        return (args, kwargs)

    def on_response(self, proxy, result, *raw_args, **raw_kwargs):
        # bind the returned dict values to the response protobuf message
        response_type = getattr(self.pb2, '{}Response'.format(
            TextTransform.camel(proxy.name)
        ))
        resp = response_type()
        if result:
            for k, v in result.items():
                setattr(resp, k, v)
        return resp

    def start(self):
        """
        Start the RPC client or server.
        """
        # build the grpc server
        self._grpc_server = grpc.server(ThreadPoolExecutor(max_workers=10))
        self._grpc_server.add_insecure_port(self.server_addr)

        # build the grpc servicer and connect with server
        self._grpc_servicer = self._grpc_servicer_factory()
        self._grpc_servicer.add_to_grpc_server(
            self._grpc_servicer, self._grpc_server
        )

        # start the server. note that it is non-blocking.
        self._grpc_server.start()

        # enter spin lock
        print('\n>>> gRPC server is running. Press ctrl+c to stop.')
        try:
            while True:
                time.sleep(32)
        except KeyboardInterrupt:
            print()
            if self._grpc_server is not None:
                print('>>> stopping grpc server...')
                self._grpc_server.stop(grace=5)
            print('>>> groodbye!')

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
        TODO: Iterate over function proxies, using request and response schemas
        to generate protobuf message and service types.
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
            try:
                with open(self._protobuf_filepath, 'w') as fout:
                    fout.write(source)
            except:
                traceback.print_exc()

        return source

    def _grpc_compile_proto_file(self, dest):
        """
        Compile the protobuf file resulting from .
        """
        include_dir = os.path.realpath(os.path.dirname(self._protobuf_filepath))
        proto_file = os.path.basename(self._protobuf_filepath)

        cmd = re.sub(r'\s+', ' ', '''
            python3 -m grpc_tools.protoc
                -I {include_dir}
                --python_out={build_dir}
                --grpc_python_out={build_dir}
                {proto_file}
        '''.format(
            include_dir=include_dir or '.',
            build_dir=dest,
            proto_file=proto_file
        ).strip())

        print(cmd)

        output = subprocess.getoutput(cmd)
        if output:
            print(output)


class GrpcFunctionProxy(FunctionProxy):
    """
    Command represents a top-level CliProgram Subparser.
    """

    def __init__(self, func, decorator):
        def build_schema(kwarg, name_suffix):
            type_name = self._msg_name_prefix + name_suffix
            if isinstance(kwarg, dict):
                return type(type_name, (Schema, ), kwarg)()
            else:
                return kwarg

        super().__init__(func, decorator)

        self._msg_name_prefix = decorator.params.get('message_name_prefix')
        if self._msg_name_prefix is None:
            self._msg_name_prefix = TextTransform.camel(self.name)

        self.request_schema = build_schema(
            decorator.params.get('request'), 'Request'
        )
        self.response_schema = build_schema(
            decorator.params.get('response'), 'Response'
        )

    def __call__(self, *raw_args, **raw_kwargs):
        return super().__call__(*(raw_args[:1]), **raw_kwargs)

    def generate_protobuf_message_types(self) -> List[Text]:
        return [
            self.request_schema.to_protobuf_message_declaration(),
            self.response_schema.to_protobuf_message_declaration(),
        ]

    def generate_protobuf_function_declaration(self) -> Text:
        return (
            'rpc {func_name}({req_msg_type}) '
            'returns ({resp_msg_type})'
            ' {{}}'.format(
                func_name=self.name,
                req_msg_type=self._msg_name_prefix + 'Request',
                resp_msg_type=self._msg_name_prefix + 'Response',
            )
        )
