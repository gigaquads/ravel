import os
import glob
import errno
import signal
import json
import sys
import inspect
import shutil
import subprocess
import time
import re
import multiprocessing as mp
import pprint

from multiprocessing import Process
from concurrent.futures import ThreadPoolExecutor
from typing import Text, List, Dict, Union
from importlib import import_module
from datetime import date, datetime
from collections import OrderedDict

import grpc
import jsbeautifier

from google.protobuf.message import Message
from appyratus.utils.string_utils import StringUtils
from appyratus.utils.func_utils import FuncUtils
from appyratus.utils.dict_utils import DictUtils, DictObject
from appyratus.utils.time_utils import TimeUtils
from appyratus.utils.template_utils import TemplateEnvironment

from ravel.util import (
    is_resource,
    is_batch,
    get_class_name,
    is_port_in_use,
)
from ravel.util.loggers import console
from ravel.util.json_schema import JsonSchemaGenerator
from ravel.app.base import Application
from ravel.schema import Schema, fields

from .grpc_method import GrpcMethod
from .grpc_client import GrpcClient
from .util import get_stripped_schema_name
from .proto import MessageGenerator
from .constants import (
    DEFAULT_HOST,
    DEFAULT_PORT,
    DEFAULT_GRACE,
    DEFAULT_IS_SECURE,
    REGISTRY_PROTO_FILE,
    PB2_MODULE_PATH_FSTR,
    PB2_GRPC_MODULE_PATH_FSTR,
    PROTOC_COMMAND_FSTR,
    PROTOC_WEB_GENERATE_CLIENT_COMMAND,
    PROTOC_WEB_GENERATE_MESSAGE_CLASSES_COMMAND,
)


class GrpcOptionsSchema(Schema):
    """
    Manifest gRPC options schema
    """
    client_host = fields.String(required=True, default=DEFAULT_HOST)
    server_host = fields.String(required=True, default=DEFAULT_HOST)
    secure_channel = fields.Bool(required=True, default=DEFAULT_IS_SECURE)
    port = fields.String(required=True, default=DEFAULT_PORT)
    grace = fields.Float(required=True, default=DEFAULT_GRACE)


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
    then putting the main thread in a sleep lock.
    """

    def __init__(self, middleware=None, *args, **kwargs):
        super().__init__(middleware=middleware, *args, **kwargs)
        self.grpc = DictObject()
        self.grpc.options = DictObject()
        self.grpc.worker_process_count = mp.cpu_count()
        self.grpc.worker_thread_pool_size = 1
        self.grpc.pb2 = None
        self.grpc.pb2_grpc = None
        self.grpc.server = None
        self.grpc.servicer = None
        self._client = None

    @property
    def client(self) -> GrpcClient:
        """
        Return a memoized GrpcClient instance. However, if the service isn't
        bootstrapped, None will be returned, as the client derives itself
        from a bootstrapped service.
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

    def build_js(self, manifest: Text, dest: Text):
        """
        Generate JavaScript files containing the porotobuf message types and
        client stub.
        """
        # TODO: move these methods into a grpc_web module
        self.bootstrap(manifest=manifest, build=False)

        # ensure template build directory exists
        dest = os.path.join(dest, 'grpc')
        try:
            os.makedirs(dest)
        except OSError as exc:
            if exc.errno != errno.EEXIST:
                raise

        self._compile_grpc_web_message_classes(dest)
        self._compile_grpc_web_client_stub(dest)
        self._render_grpc_web_client_files(dest)

    def _render_grpc_web_client_files(self, dest):
        dest = os.path.realpath(dest)

        # generate JSON schemas for all ravel schemas
        # referenced in this app
        def unnest(schema, unnested):
            for child_schema in schema.children:
                child_schema_name = get_stripped_schema_name(child_schema)
                if child_schema_name in unnested:
                    console.warning(
                        message=(
                            f'gRPC web client build process '
                            f'ignoring schema with duplicate name'
                        ),
                        data={
                            'schema': child_schema_name,
                            'parent_schema': get_stripped_schema_name(schema),
                        }
                    )
                unnested[child_schema_name] = schema
                unnest(child_schema, unnested)

        nested_schemas = OrderedDict()
        for resource_type in self.res.values():
            unnest(resource_type.ravel.schema, nested_schemas)

        action_schemas = OrderedDict()
        for action in self.actions.values():
            for schema in action.schemas.values():
                action_schemas[get_stripped_schema_name(schema)] = schema

        # initialize templating engine
        template_dir = os.path.join(os.path.dirname(__file__), 'templates')
        jinja = TemplateEnvironment(template_dir)
        template_context = {
            'list': list,
            'dict': dict,
            'str': str,
            'int': int,
            'set': set,
            'len': len,
            'app': self,
            'inspect': inspect,
            'nested_schemas': nested_schemas,
            'action_schemas': action_schemas,
            'json_schema_generator': JsonSchemaGenerator(),
            'class_name': get_class_name,
        }

        console.info(
            message=f'jinja2 template directory',
            data={'path': template_dir}
        )

        # render templates
        for js_fname in glob.glob(template_dir + '/*.js'):
            js_fname = os.path.basename(js_fname)
            with open(os.path.join(dest, js_fname), 'w') as js_file:
                # render template from file
                template = jinja.from_filename(js_fname)
                js_source = template.render(template_context)
                # lint the generated JS code
                formated_js_source = jsbeautifier.beautify(js_source)
                # write to dest directory
                console.info(f'rendering {dest}/{os.path.basename(js_fname)}')
                js_file.write(formated_js_source)

    def on_decorate(self, action):
        pass

    def on_request(self, action, request, *args, **kwargs):
        """
        Take the attributes on the incoming protobuf Message object and
        map them to the args and kwargs expected by the action target.
        """
        console.debug(
            message=f'RPC method {action.name} requested',
        )

        # get field data to process into args and kwargs
        def deserialize_value(field, value):
            if isinstance(field, fields.Dict):
                if not value:
                    value = None if field.nullable else {}
                else:
                    value = self.json.decode(value)
            return value

        all_arguments = {}
        for field in action.schemas.request.fields.values():
            value = getattr(request, field.name, None)
            all_arguments[field.name] = deserialize_value(field, value)

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
        response_type = self.grpc.response_types[action.name]
        response = response_type()
        response_data = None

        def validate(data):
            data, errors = schema.process(data)
            if errors:
                console.error(
                    message=f'response validation errors',
                    data={
                        'data': pprint.pformat(data),
                        'errors': errors,
                    }
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

    def on_start(self):
        """
        Start the RPC client or server.
        """
        # try to ensure the desired port is available
        if is_port_in_use(self.grpc.options.server_address):
            console.critical(
                f'gRPC service address {self.grpc.options.server_address} '
                f'in use. trying to kill stale service processes.'
            )
            server_port = self.grpc.options.port
            try:
                kill_cmd = f'lsof -ti tcp:{server_port} | xargs kill'
                console.info(
                    message=f'running command to free gRPC service port',
                    data={'port': server_port, 'command': kill_cmd}
                )
                subprocess.getoutput(kill_cmd)
            except Exception:
                console.exception(
                    f'error running command to free gRPC service port'
                )
                exit(-1)
            if is_port_in_use(self.grpc.options.server_address):
                console.critical('could not start gRPC service')
                exit(-1)

        console.info(
            message='strating gRPC server. Press ctrl+c to stop.',
            data={
                'address': self.grpc.options.server_address,
                'methods': list(self.actions.keys()),
                'worker_process_count': self.grpc.worker_process_count,
                'worker_thread_pool_size': self.grpc.worker_thread_pool_size,
            }
        )
        # start the server process/es
        if self.grpc.worker_process_count == 1:
            # serve in main forground process
            self._init_server_and_run()
        else:
            # serve in forked subprocesses
            self._spawn_worker_processes()

    def _spawn_worker_processes(self):
        processes = []

        # entrypoint or "target" function for server processes
        def entrypoint(service):
            service.bootstrap(force=True)
            service._init_server_and_run()

        # create and run the server processes
        for _ in range(self.grpc.worker_process_count):
            process = Process(target=entrypoint, args=(self, ))
            processes.append(process)
            process.start()

        self._setup_os_signal_handling_when_multiprocessing()

        for process in processes:
            process.join(timeout=5)

    def _setup_os_signal_handling_when_multiprocessing(self):
        def handler(signum, frame):
            console.debug(
                'ignoring SIGINT in root process. joining '
                'all server subprocesses.'
            )
        # set the signal handler
        signal.signal(signal.SIGINT, handler)

    def _init_server_and_run(self):
        # the grpc server runs in a thread pool
        self.grpc.executor = ThreadPoolExecutor(
            max_workers=self.grpc.worker_thread_pool_size,
            initializer=self.on_start_worker,
            initargs=(self, )
        )

        for i in range(self.grpc.worker_thread_pool_size):
            self.grpc.executor.submit(lambda: None)

        # build grpc server
        self.grpc.server = grpc.server(self.grpc.executor)
        self.grpc.server.add_insecure_port(self.grpc.options.server_address)

        # build grpcio "servicer" class
        self.grpc.servicer = self._grpc_servicer_factory()
        self.grpc.servicer.add_to_grpc_server(
            self.grpc.servicer, self.grpc.server
        )

        self.grpc.server.start()

        # sleep-lock the main thread to keep the server running,
        # as the server runs as a daemon.
        try:
            while True:
                time.sleep(9999)
        except KeyboardInterrupt:
            # stop server on ctrl+c
            if self.grpc.server is not None:
                console.info(f'stopping gRPC server (PID: {os.getpid()})')
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
        schema = GrpcOptionsSchema()
        options, errors = schema.process(computed_options)
        if errors:
            raise Exception(f'invalid gRPC options: {errors}')

        grpc.options = DictObject(options)
        grpc.options.port = port = options['port']
        grpc.options.server_host = server_host = options['server_host']
        grpc.options.client_host = client_host = options['client_host']
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
        necessary for running this service. Then we import them, because we
        need to derive client and servicer subclasses and procedures from the
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
        grpc.response_types = {}
        for action in self.actions.values():
            schema_type_name = get_class_name(action.schemas.response)
            message_type_name = get_stripped_schema_name(
                StringUtils.camel(schema_type_name)
            )
            response_message_type = getattr(self.grpc.pb2, message_type_name)
            grpc.response_types[action.name] = response_message_type

    def _grpc_servicer_factory(self):
        """
        Import the abstract base Servicer class from the autogenerated
        pb2_grpc module and derive a new subclass that inherits this
        Application's action objects as its interface implementation.
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
        servicer_type = type(
            servicer_type_name, (abstract_type, ), self.actions.to_dict()
        )
        servicer = servicer_type()

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
        dot_file_name = os.path.join(self.grpc.build_dir, '.ravel')
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
        # all lines in proto source code accumulated into "lines"
        lines = ['syntax = "proto3";']

        # service interface function delaration string accumulator
        decls = []

        # generate proto messages for each Resource class in this app
        msg_gen = MessageGenerator()
        for name, resource_type in self.res.items():
            lines.append(msg_gen.emit_resource_message(resource_type) + '\n')

        # generate request and response schemas along with service
        # interface function declarations, derived them application actions
        for action in self.actions.values():
            decls.append(action.generate_protobuf_function_declaration())
            if action.schemas.request:
                lines.append(action.generate_request_message_type())
            if action.schemas.response:
                lines.append(action.generate_response_message_type())

        # assemble service interface definition block
        lines.append('service GrpcApplication {')
        for decl in decls:
            lines.append('  ' + decl)
        lines.append('}\n')

        # write final protobuf source code to file
        console.info(
            message='generating gRPC proto file',
            data={'destination': self.grpc.proto_file}
        )
        with open(self.grpc.proto_file, 'w') as fout:
            fout.write('\n'.join(lines))

    def _compile_pb2_modules(self):
        """
        Compile the grpc .proto file, generating pb2 and pb2_grpc modules in
        the build directory. These modules contain abstract base classes
        required by the grpc server and client.
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

    def _compile_grpc_web_message_classes(self, dest: Text):
        """
        Compile the grpc .proto file, generating pb2 and pb2_grpc modules in
        the build directory. These modules contain abstract base classes
        required by the grpc server and client.
        """
        # build the shell command to run....
        protoc_command = PROTOC_WEB_GENERATE_MESSAGE_CLASSES_COMMAND.format(
            include_dir=os.path.realpath(
                os.path.dirname(self.grpc.proto_file)
            ) or '.',
            build_dir=dest,
            proto_file=os.path.basename(self.grpc.proto_file),
        ).strip()

        console.info(
            message='generating gRPC-web message classes',
            data={'command': protoc_command.split('\n')}
        )
        err_msg = subprocess.getoutput(
            re.sub(r'\s+', ' ', protoc_command)
        )
        if err_msg:
            exit(err_msg)

    def _compile_grpc_web_client_stub(self, dest: Text):
        """
        Compile the grpc .proto file, generating pb2 and pb2_grpc modules in
        the build directory. These modules contain abstract base classes
        required by the grpc server and client.
        """
        # build the shell command to run....
        protoc_command = PROTOC_WEB_GENERATE_CLIENT_COMMAND.format(
            include_dir=os.path.realpath(
                os.path.dirname(self.grpc.proto_file)
            ) or '.',
            build_dir=dest,
            proto_file=os.path.basename(self.grpc.proto_file),
        ).strip()

        console.info(
            message='generating gRPC-web client stub',
            data={'command': protoc_command.split('\n')}
        )
        err_msg = subprocess.getoutput(
            re.sub(r'\s+', ' ', protoc_command)
        )
        if err_msg:
            exit(err_msg)

    def build_response_message(
        self,
        message: Message,
        source: Dict
    ) -> Message:
        """
        Recursively bind each value in a source dict to the corresponding
        attribute in a grpc Message object, resulting in the prepared response
        message, ready to be sent back to the client.
        """
        if source is None:
            return None

        for k, v in source.items():
            if v is None:
                # protobufs don't understand null values
                continue

            # TODO: move conversion logic into adapters
            if isinstance(v, (date, datetime)):
                ts = TimeUtils.to_timestamp(v)
                setattr(message, k, ts)
            elif isinstance(getattr(message, k), Message):
                sub_message = getattr(message, k)
                assert isinstance(v, dict)
                self.build_response_message(sub_message, v)
            elif isinstance(v, dict):
                json_str = self.json.encode(v)
                setattr(message, k, json_str)
            elif isinstance(v, (list, tuple, set)):
                list_field = getattr(message, k)
                sub_message_type_name = (
                    '{}Schema'.format(k.title().replace('_', ''))
                )
                sub_message_type = getattr(
                    message, sub_message_type_name, None
                )
                if sub_message_type:
                    list_field.extend(
                        self.build_response_message(sub_message_type(), v_i)
                        for v_i in v
                    )
                else:
                    v_prime = [
                        x if not isinstance(x, dict) else self.json.encode(x)
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
