import os
import sys
import traceback
import importlib
import inspect
import subprocess
import re

import grpc

from concurrent import futures
from typing import Text, List

from appyratus.validation.schema import Schema
from appyratus.util import TextTransform

from .base import FunctionRegistry, FunctionDecorator, FunctionProxy


class GrpcRegistry(FunctionRegistry):
    """
    Grpc server and client interface.
    """

    def __init__(self):
        super().__init__()
        self._proxies = []
        self._grpc_server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))

    def bootstrap(self, manifest_filepath: Text, build_grpc=False):
        super().bootstrap(manifest_filepath)
        self._pkg_path = self.manifest.package
        self._pkg = importlib.import_module(self._pkg_path)
        self._pkg_dir = os.path.dirname(self._pkg.__file__)
        self._proto_file = os.path.join(self._pkg_dir, 'registry.proto')
        self._pb2_mod_path = '{}.registry_pb2'.format(self._pkg_path)
        self._pb2_grpc_mod_path = '{}.registry_pb2_grpc'.format(self._pkg_path)


        self._grpc_options = self.manifest.data.get('grpc', {})
        self._grpc_server_addr = '[{}]:{}'.format(
            self._grpc_options.get('host', '::'),
            self._grpc_options.get('port', '50051')
        )

        self._grpc_server.add_insecure_port(self._grpc_server_addr)

        if build_grpc:
            self._grpc_build()

    @property
    def function_proxy_type(self):
        return GrpcFunctionProxy

    def on_decorate(self, proxy):
        """
        Collect each FunctionProxy.
        """
        self._proxies.append(proxy)

    def on_request(self, proxy, signature, grpc_request, grpc_context):
        """
        Extract command line arguments and bind them to the arguments expected
        by the registered function's signature.
        """

    def start(self):
        """
        Start the RPC client or server.
        """
        self.grpc_servicer = self._servicer_factory()
        self._grpc_server.start()
        print('>>> gRPC server is running...')
        while True:
            cmd = input('<<< ').strip().lower()
            if cmd == 'q':
                print('>>> stopping...')
                if self._grpc_server is not None:
                    self._grpc_server.stop(grace=5)
                break

    def _servicer_factory(self):
        sys.path.append(self._pkg_dir)
        pb2_grpc = importlib.import_module(
            self._pb2_grpc_mod_path,
            self._pkg_path
        )
        servicer_type_name = 'GrpcRegistryServicer'
        abstract_type = None

        for k, v in inspect.getmembers(pb2_grpc):
            if k == servicer_type_name:
                abstract_type = v
                break

        if abstract_type is None:
            raise Exception('could not find grpc Servicer class')

        methods = {p.target_name: p for p in self._proxies}
        servicer_type = type(servicer_type_name, (abstract_type, ), methods)
        servicer = servicer_type()

        pb2_grpc.add_GrpcRegistryServicer_to_server(servicer, self._grpc_server)

        return servicer

    def _grpc_build(self):
        self._grpc_generate_proto_file()
        self._grpc_compile_proto_file()

    def _grpc_generate_proto_file(self):
        """
        TODO: Iterate over function proxies, using request and response schemas
        to generate protobuf message and service types.
        """
        dest = self._pkg_dir
        chunks = ['syntax = "proto3";']
        func_decls = []
        for proxy in self._proxies:
            chunks.extend(proxy.generate_protobuf_message_types())
            func_decls.append(proxy.generate_protobuf_function_declaration())

        chunks.append('service GrpcRegistry {')
        for decl in func_decls:
            chunks.append('  ' + decl)
        chunks.append('}\n')

        source = '\n'.join(chunks)

        if self._proto_file:
            try:
                with open(self._proto_file, 'w') as fout:
                    fout.write(source)
            except:
                traceback.print_exc()

        return source

    def _grpc_compile_proto_file(self):
        """
        Compile the protobuf file resulting from .
        """
        dest = self._pkg_dir
        include_dir = os.path.realpath(os.path.dirname(self._proto_file))
        proto_file = os.path.basename(self._proto_file)

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
            self._msg_name_prefix = TextTransform.camel(self.target_name)

        self.request_schema = build_schema(
            decorator.params.get('request'), 'Request'
        )
        self.response_schema = build_schema(
            decorator.params.get('response'), 'Response'
        )

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
                func_name=self.target_name,
                req_msg_type=self._msg_name_prefix + 'Request',
                resp_msg_type=self._msg_name_prefix + 'Response',
            )
        )
