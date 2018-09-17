import os
import sys
import traceback
import importlib
import inspect
import subprocess
import re

import grpc

from concurrent.futures import ThreadPoolExecutor
from typing import Text, List
from importlib import import_module

from appyratus.validation.schema import Schema
from appyratus.decorators import memoized_property
from appyratus.util import TextTransform, FuncUtils

from .base import FunctionRegistry, FunctionDecorator, FunctionProxy


class GrpcRegistry(FunctionRegistry):
    """
    Grpc server and client interface.
    """

    def __init__(self):
        super().__init__()
        self._grpc_server = None
        self._grpc_servicer = None

    def bootstrap(self, manifest_filepath: Text, build_grpc=False):
        super().bootstrap(manifest_filepath)
        self._pkg_path = self.manifest.package
        self._pkg = importlib.import_module(self._pkg_path)
        self._pkg_dir = os.path.dirname(self._pkg.__file__)
        self._proto_file = os.path.join(self._pkg_dir, 'registry.proto')
        self._pb2_mod_path = '{}.registry_pb2'.format(self._pkg_path)
        self._pb2_grpc_mod_path = '{}.registry_pb2_grpc'.format(self._pkg_path)

        sys.path.append(self._pkg_dir)

        self.pb2 = import_module(self._pb2_mod_path, self._pkg_path)
        self.pb2_grpc = import_module(self._pb2_grpc_mod_path, self._pkg_path)

        self.grpc_options = self.manifest.data.get('grpc', {})

        server_host = self.grpc_options.get('server-host', '::')
        client_host = self.grpc_options.get('client-host', 'localhost')
        port = self.grpc_options.get('port', '50051')

        self.grpc_server_addr = '[{}]:{}'.format(server_host, port)
        self.grpc_client_addr = '{}:{}'.format(client_host, port)

        if build_grpc:
            self.grpc_build()

    @property
    def function_proxy_type(self):
        return GrpcFunctionProxy

    @memoized_property
    def client(self):
        return GrpcClient(self)

    def on_decorate(self, proxy):
        pass

    def on_request(self, proxy, signature, grpc_request):
        """
        Extract command line arguments and bind them to the arguments expected
        by the registered function's signature.
        """ 
        data = {
            k: getattr(grpc_request, k, None)
            for k in proxy.request_schema.fields
        }
        args, kwargs = FuncUtils.extract_arguments(signature, data)
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
        self._grpc_server = grpc.server(ThreadPoolExecutor(max_workers=10))
        self._grpc_servicer = self._grpc_servicer_factory()
        self._grpc_servicer.add_to_grpc_server(
            self._grpc_servicer, self._grpc_server
        )
        self._grpc_server.add_insecure_port(self.grpc_server_addr)
        self._grpc_server.start()
        print('>>> gRPC server is running...')
        while True:
            cmd = input('<<< ').strip().lower()
            if cmd == 'q':
                print('>>> stopping...')
                if self._grpc_server is not None:
                    self._grpc_server.stop(grace=5)
                break

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

    def grpc_build(self):
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
        for proxy in self.proxies:
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


class GrpcClient(object):
    def __init__(self, registry: GrpcRegistry):
        assert registry.is_bootstrapped
        self._registry = registry
        self._channel = grpc.insecure_channel(registry.grpc_client_addr)
        self._grpc_stub = registry.pb2_grpc.GrpcRegistryStub(channel)
        self._funcs = {p.name: self._build_func(p) for p in registry.proxies}

    def __getattr__(self, func_name: Text):
        return self._funcs[func_name]

    def _build_func(self, proxy):
        key = TextTransform.camel(proxy.name)
        request_type = getattr(self.pb2, '{}Request'.format(key))
        send = getattr(self._grpc_stub, proxy.name)

        def func(self, **kwargs):
            req = request_type(**kwargs)
            resp = send(req)
            return {
                k: getattr(resp, k, None)
                for k in proxy.response_schema.fields
            }

        return func
