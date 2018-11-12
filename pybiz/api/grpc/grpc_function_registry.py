import os
import sys
import traceback
import importlib
import socket
import inspect
import subprocess
import time
import re
import pickle
import codecs

import grpc

from concurrent.futures import ThreadPoolExecutor
from typing import Text, List, Dict, Type
from collections import deque
from importlib import import_module

from google.protobuf.message import Message

from appyratus.schema import Schema
from appyratus.schema.fields import Field
from appyratus.schema import fields
from appyratus.decorators import memoized_property
from appyratus.util import TextTransform, FuncUtils
from appyratus.json import JsonEncoder

from ..base import FunctionRegistry, FunctionDecorator, FunctionProxy
from .grpc_client import GrpcClient
from .grpc_remote_dao import remote_dao_endpoint_factory


class GrpcFunctionRegistry(FunctionRegistry):
    """
    Grpc server and client interface.
    """

    def __init__(self, middleware=None):
        super().__init__(middleware=middleware)
        self._json_encoder = JsonEncoder()
        self._grpc_server = None
        self._grpc_servicer = None

    def bootstrap(self, manifest_filepath: Text, build_grpc=False):
        super().bootstrap(manifest_filepath, defer_processing=True)

        pkg_path = self.manifest.package
        pkg = importlib.import_module(pkg_path)
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

        def onerror(name):
            if issubclass(sys.exc_info()[0], ImportError):
                pass

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
            for k, v in data.items():
                if isinstance(v, Message):
                    recurseively_bind(getattr(target, k), v)
                else:
                    setattr(target, k, v)

        # bind the returned dict values to the response protobuf message
        response_type = getattr(
            self.pb2, '{}Response'.format(TextTransform.camel(proxy.name))
        )
        resp = response_type()
        if result:
            dumped_result = proxy.response_schema.dump(
                result, strict=True
            ).data
            for k, v in dumped_result.items():
                field = proxy.response_schema.fields[k]
                if isinstance(field, fields.Dict):
                    v_bytes = codecs.encode(pickle.dumps(v), 'base64')
                    setattr(resp, k, v_bytes)
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
            #initializer=initializer, # XXX TypeError: __init__() got an unexpected keyword argument 'initializer'
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


class GrpcFunctionProxy(FunctionProxy):
    """
    Command represents a top-level CliProgram Subparser.
    """

    def __init__(self, func, decorator):
        def build_schema(kwarg, name_suffix):
            type_name = self._msg_name_prefix + name_suffix
            if isinstance(kwarg, dict):
                return Schema.factory(type_name, kwarg)()
            else:
                return kwarg

        super().__init__(func, decorator)

        self.msg_gen = MessageGenerator()
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
            self.msg_gen.emit(self.request_schema),
            self.msg_gen.emit(self.response_schema),
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


class FieldAdapter(object):
    def __init__(self):
        self.msg_gen = None

    def emit(self, field, field_no):
        raise NotImplementedError()

    def bind(self, msg_gen: 'MessageGenerator'):
        self.msg_gen = msg_gen

    def emit(self, field_type, field_no, field_name, is_repeated=False):
        return '{repeated} {field_type} {field_name}{field_no}'.format(
            repeated=' repeated' if is_repeated else '',
            field_type=field_type,
            field_name=field_name,
            field_no=' = {}'.format(field_no) if field_no < 16 else '',
        )


class ScalarFieldAdapter(FieldAdapter):
    def __init__(self, type_name):
        self.type_name = type_name

    def emit(self, field, field_no):
        field_type = self.msg_gen.get_adapter(field).type_name
        return super().emit(
            field_type=field_type,
            field_no=field_no,
            field_name=field.name
        )


class ArrayFieldAdapter(FieldAdapter):
    def emit(self, field, field_no):
        if isinstance(field.nested, Schema):
            nested_field_type = field.nested.__class__.__name__
        else:
            adapter = self.msg_gen.get_adapter(field.nested)
            nested_field_type = adapter.type_name
        return super().emit(
            field_type=nested_field_type,
            field_no=field_no,
            field_name=field.name,
            is_repeated=True,
        )


class NestedFieldAdapter(FieldAdapter):
    def emit(self, field, field_no):
        return super().emit(
            field_type=field.schema_type.__name__,
            field_no=field_no,
            field_name=field.name,
        )


class SchemaFieldAdapter(FieldAdapter):
    def emit(self, field, field_no):
        return super().emit(
            field_type=field.__class__.__name__,
            field_no=field_no,
            field_name=field.name,
        )


class EnumFieldAdapter(FieldAdapter):
    def emit(self, field, field_no):
        nested_field_type = self.msg_gen.get_adapter(field.nested).type_name
        return super().emit(
            field_type=nested_field_type,
            field_no=field_no,
            field_name=field.name,
        )


class MessageGenerator(object):
    def __init__(self, adapters: Dict[Type[Field], FieldAdapter]=None):
        # default field adapters indexed by appyratus schema Field types
        self.adapters = {
            Schema: SchemaFieldAdapter(),
            fields.Nested: NestedFieldAdapter(),
            fields.String: ScalarFieldAdapter('string'),
            fields.FormatString: ScalarFieldAdapter('string'),
            fields.Email: ScalarFieldAdapter('string'),
            fields.Uuid: ScalarFieldAdapter('string'),
            fields.Bool: ScalarFieldAdapter('bool'),
            fields.Float: ScalarFieldAdapter('double'),
            fields.Int: ScalarFieldAdapter('sint64'),
            fields.DateTime: ScalarFieldAdapter('uint64'),
            fields.Dict: ScalarFieldAdapter('bytes'),
            fields.List: ArrayFieldAdapter(),
            # XXX redundant to List?  does not exist in schema.fields
            #fields.Array: ArrayFieldAdapter(),
            # XXX do we add?  does not exist in schema.fields
            #fields.Enum: EnumFieldAdapter(),
            # XXX do we add?  does not exist in schema.fields
            #fields.Regexp: ScalarFieldAdapter('string'),
        }
        # upsert into default adapters dict from the `adapters` kwarg
        for field_type, adapter in (adapters or {}).items():
            self.adapters[field_type] = adapter
        # associate the generator with each adapter.
        for adapter in self.adapters.values():
            adapter.bind(self)

    def get_adapter(self, field) -> FieldAdapter:
        if isinstance(field, Schema):
            return self.adapters[Schema]
        else:
            return self.adapters[field.__class__]

    def emit(
        self,
        schema_type: Type['Schema'],
        type_name: Text = None,
        depth=1
    ) -> Text:
        """
        Recursively generate a protocol buffer message type declaration string
        from a given Schema class.
        """
        if isinstance(schema_type, type):
            type_name = type_name or schema_type.__name__
        elif isinstance(schema_type, Schema):
            type_name = type_name or schema_type.__class__.__name__
        else:
            raise ValueError(
                'unrecognized schema type: "{}"'.format(schema_type)
            )

        field_no2field = {}
        prepared_data = []
        field_decls = []

        for f in schema_type.fields.values():
            # compute the "field number"
            field_no = f.meta.get('field_no', sys.maxsize)

            # get the field adapter
            adapter = self.get_adapter(f)
            if not adapter:
                raise Exception('no adapter for type {}'.format(f.__class__))

            # store in intermediate data structure for purpose of sorting by
            # field numbers
            prepared_data.append((field_no, f, adapter))

        # emit field declarations in order of field number ASC
        sorted_data = sorted(prepared_data, key=lambda x: x[0])
        for (field_no, field, adapter) in sorted_data:
            field_decl = adapter.emit(field, field_no)
            field_decls.append(('  ' * depth) + field_decl + ';')

        nested_message_types = [
            self.emit(nested_schema, depth=depth + 1)
            for nested_schema in {
                s.__class__ for s in schema_type.children
            }
        ]

        MESSAGE_TYPE_FSTR = (
            (('   ' * (depth - 1)) + '''message {type_name} ''') + \
            ('''{{\n{nested_message_types}{field_lines}\n''') + \
            (('   ' * (depth - 1)) + '''}}''')
        )

        # emit the message declaration "message Foo { ... }"
        return MESSAGE_TYPE_FSTR.format(
            type_name=type_name,
            nested_message_types=(
                '\n'.join(nested_message_types) + '\n'
                if nested_message_types else ''
            ),
            field_lines='\n'.join(field_decls),
        ).rstrip()
