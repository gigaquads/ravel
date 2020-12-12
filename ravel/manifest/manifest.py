import os
import re
import traceback
import pkg_resources
import concurrent.futures

from collections import deque
from concurrent.futures import ThreadPoolExecutor
from threading import local
from typing import Text, Dict, Type, Union, Set, List
from copy import deepcopy

from appyratus.utils.dict_utils import DictUtils, DictObject
from appyratus.utils.string_utils import StringUtils
from appyratus.files import Yaml, Json
from appyratus.env import Environment

from ravel.exceptions import ManifestError
from ravel.util.misc_functions import import_object, get_class_name
from ravel.util.loggers import console
from ravel.util.scanner import Scanner
from ravel.schema import Schema, fields

from .scanner import ManifestScanner
from .bootstrap import Bootstrap
from .binding import Binding
from .exceptions import *



class Manifest:

    class Schema(Schema):
        base = fields.FilePath()
        package = fields.String()
        bindings = fields.List(Binding.Schema(), default=[])
        bootstraps = fields.List(Bootstrap.Schema(), default=[])
        values = fields.Dict(default={})

    def __init__(self, source: Union['Manifest', Dict, Text]):
        filepath, data = self._load_manifest_data(source)

        self.env = Environment()
        self.filepath = filepath
        self.data = data
        self.bindings = []
        self.bootstraps = []
        self.default_bootstrap = None
        self.scanner = None
        self.app = None

    def bootstrap(
        self,
        app: 'Application',
        namespace: Dict = None
    ) -> 'Manifest':
        self.app = app
        self.bootstraps = self._initialize_bootstraps()
        self.bindings = self._initialize_bindings()
        self.scanner = self._scan_filesystem()

        if namespace:
            self._scan_namespace_dict(namespace)

        self._bootstrap_store_classes()
        self._bind_resources()
        self._bootstrap_resource_classes()
        self._inject_classes_to_actions()
        self._bootstrap_actions()

    def register(
        self,
        resource_classes: Union['Resource', List[Type['Resource']]],
        store_class: Type['Store'] = None
    ):
        """
        Dynamically register and bootstrap a resource with its associated
        store class.
        """
        from ravel import Resource

        def resolve_store_class(resource_class, store_class):
            if not store_class:
                if self.default_bootstrap:
                    return self.default_bootstrap.store_class
                else:
                    return resource_class.__store__()
            else:
                return store_class

        def validate_resource_class(resource_class):
            if not isinstance(resource_class, Resource):
                raise TypeError(
                    f'{get_class_name(resource_class)} must '
                    f'be a Resource subclass'
                )
            if resource_class in self.resource_classes:
                raise DuplicateResourceClass(
                    f'{get_class_name(resource_class)} is already registerd '
                    f'with the application'
                )
            return resource_class

        def create_and_add_binding(resource_class, store_class):
            resource_class_name = get_class_name(resource_class)
            store_class_name = get_class_name(store_class)
            binding = Binding(resource_class_name, store_class_name)
            binding.resource_class = resource_class
            binding.store_class = store_class
            self.bindings.append(binding)
            return binding

        def register_classes(resource_class, store_class):
            # add the resource class to global register if it's new
            resource_class_name = get_class_name(resource_class)
            if resource_class_name not in self.resource_classes:
                self.resource_classes[resource_class] = resource_class

            # add the store class to global register if it's new
            store_class_name = get_class_name(store_class)
            if store_class_name not in self.store_classes:
                self.store_classes[store_class_name] = store_class

        def bootstrap_new_classes(self, resource_class, store_class):
            resource_class_name = get_class_name(resource_class)
            store_class_name = get_class_name(store_class)
            bootstraps = {
                x.store_class_name: x for x in self.bootstraps
            }
            if self.app.is_bootstrapped:
                bootstrap = self.bootstraps.get(store_class_name)
                params = bootstrap.params if bootstrap else {}
                if not store_class.is_bootstrapped():
                    store_class.bootstrap(self.app, **params)
                if not resource_class.is_bootstraped():
                    resource_class.bootstrap(self.app, **params)

        # make sure we don't already have a store class registered
        if store_class in self.store_classes:
            raise DuplicateStoreClass(
                f'{get_class_name(store_class)} is already registerd '
                f'with the application'
            )

        # register new classes and bootstrap
        for resource_class in resource_classes:
            resource_class = validate_resource_class(resource_class)
            store_class = resolve_store_class(resource_class, store_class)

            register_classes(resource_class, store_class)

            binding = create_and_add_binding(resource_class, store_class)

            # bind new store singleton to resource_class
            store = store_class()
            resource_class.bind(store)
            console.debug(
                f'binding thread-local {binding.store_class_name}() '
                f'to {binding.resource_class_name} class'
            )
            store.bind(resource_class)

            # bootstrap the resource and store classes if they are
            # not alredy but the host app is.
            bootstrap_new_classes(resource_class, store_class)

    @property
    def package(self) -> Text:
        return self.data.get('package')

    @property
    def resource_classes(self) -> Dict[Text, Type['Resource']]:
        if self.scanner is not None:
            return self.scanner.context.resource_classes
        return DictObject()

    @property
    def store_classes(self) -> Dict[Text, Type['Store']]:
        if self.scanner is not None:
            return self.scanner.context.store_classes
        return DictObject()

    @property
    def actions(self) -> Dict[Text, 'Action']:
        if self.scanner is not None:
            return self.scanner.context.actions
        return DictObject()

    def _bind_resources(self):
        def load(binding):
            b = binding

            resource_class = self.resource_classes.get(b.resource_class_name)
            b.resource_class = resource_class
            if resource_class is None:
                raise ResourceClassNotFound(b.resource_class_name)

            store_class = self.store_classes.get(b.store_class_name)
            b.store_class = store_class
            if store_class is None:
                raise StoreClassNotFound(b.store_class_name)

            return binding

        # if the abstract base Resource class is present, dispose with it!
        self.resource_classes.pop('Resource', None)

        for binding in self.bindings:
            binding = load(binding)
            console.debug(
                f'binding thread-local {binding.store_class_name}() '
                f'to {binding.resource_class_name} class'
            )
            store = binding.store_class()
            store.bind(binding.resource_class, **binding.bind_params)
            binding.resource_class.bind(store, **binding.bind_params)

    def _bootstrap_resource_classes(self):
        app = self.app
        for binding in self.bindings:
            binding.resource_class.bootstrap(app)

    def _inject_classes_to_actions(self):
        for action in self.actions:
            self.app.inject(app.target)

    def _bootstrap_actions(self):
        for action in self.actions.values():
            action.bootstrap()

        on_parts = []
        pre_parts = []
        post_parts = deque()

        def is_virtual(func):
            return getattr(func, 'is_virtual', None)

        # middleware bootstrapping...
        for idx, mware in enumerate(self.app.middleware):
            mware.bootstrap(app=self)

            # everything below is for generating the log message
            # containing the "action execution diagram"
            name = StringUtils.snake(get_class_name(mware))
            if not is_virtual(mware.pre_request):
                pre_parts.append(
                    f"↪ {name}.pre_request(raw_args, raw_kwargs)"
                )

            if not is_virtual(mware.on_request):
                on_parts.append(
                    f"↪ {name}.on_request(args, kwargs)"
                )
            if not is_virtual(mware.post_request):
                if is_virtual(mware.post_bad_request):
                    post_parts.appendleft(
                        f"↪ {name}.post_request(result)"
                    )
                else:
                    post_parts.appendleft(
                        f"↪ {name}.post_[bad_]request(result|error)"
                    )
            elif not is_virtual(mware.post_bad_request):
                post_parts.appendleft(
                    f"↪ {name}.post_bad_request(error)"
                )

        parts = []
        parts.append(
            '➥ app.on_request(action, *raw_args, **raw_kwargs)'
        )
        parts.extend(pre_parts)
        parts.append(
            '➥ args, kwargs = action.marshal(raw_args, raw_kwargs)'
        )
        parts.extend(on_parts)
        parts.append(
            '➥ raw_result = action(*args, **kwargs)'
        )
        parts.extend(post_parts)
        parts.append(
            '➥ return app.on_response(action, raw_result)'
        )

        diagram = '\n'.join(
            f'{"  " * (i+1)}{s}' for i, s in enumerate(parts)
        )
        console.debug(
            message=(
                f"action execution diagram...\n\n {diagram}\n"
            )
        )

    def _bootstrap_store_classes(self):
        app = self.app
        bootstraps = self.bootstraps
        store_classes = self.store_classes

        # if the abstract base Store class is present, dispose with it!
        self.store_classes.pop('Store', None)

        for bootstrap in bootstraps:
            store_class = store_classes.get(bootstrap.store_class_name)
            if store_class is None:
                raise StoreClassNotFound(
                    f'cannot find {bootstrap.store_class_name}'
                )
            else:
                console.debug(
                    f'bootstrapping {bootstrap.store_class_name}'
                )
                store_class.bootstrap(app)

    def _initialize_bootstraps(self):
        return [
            Bootstrap(x['store'], x.get('params'), x.get('default'))
            for x in self.data.get('bootstraps', [])
        ]

    def _initialize_bindings(self):
        default_bootstrap = None
        store_class_names = set()
        bootstraps = self.bootstraps
        bindings = []

        if bootstraps:
            for x in bootstraps:
                store_class_names.add(x.store_class_name)
                if x.is_default:
                    default_bootstrap = x

        self.default_bootstrap = default_bootstrap

        for x in self.data.get('bindings', []):
            store_class_name = x.get('store')
            if not store_class_name:
                if default_bootstrap:
                    store_class_name = default_bootstrap.store_class_name
                else:
                    store_class_name = None

            params = x.get('params') or {}
            binding = Binding(x['resource'], store_class_name, params)
            bindings.append(binding)

            if binding.store_class_name not in store_class_names:
                bootstrap = Bootstrap(binding.store_class_name)
                bootstraps.append(bootstrap)

        return bindings

    @classmethod
    def _load_manifest_data(cls, source):
        schema = cls.Schema(allow_additional=True)
        filepath = None
        data = {}

        # load the manifest data dict differently, depending
        # on its source -- e.g. from a file path, a dict, another manifest
        if isinstance(source, Manifest):
            data = deepcopy(source.data)
            filepath = source.filepath
        elif isinstance(source, str):
            filepath = os.path.abspath(
                os.path.expandvars(os.path.expanduser(source))
            )
            if os.path.isfile(source):
                console.debug(f'reading manifest from {source}')
                data = cls._read_file(source)
            else:
                raise ManifestFileNotFound(
                    f'manifest file {filepath} not found. '
                    f'yaml and json manifest file types are supported.'
                )
        elif isinstance(source, dict):
            data = cls._expand_vars(source)
            filepath = None

        # merge data dict into recursively inherited data dict
        base_filepath = data.get('base')
        if base_filepath:
            inherited_data = inherit_base_manifest(base_filepath)
            data = DictUtils.merge(inherited_data, data)

        # validate final computed data dict
        validated_data, errors = schema.process(data)
        if errors:
            raise ManifestValidationError(
                f'manifest validation error/s: {errors}'
            )

        return (filepath, validated_data)

    @classmethod
    def _read_file(cls, path: Text) -> Dict:
        ext = os.path.splitext(path)[-1].lower().lstrip('.')
        if ext in ('yaml', 'yml'):
            return cls._expand_vars(Yaml.read(path) or {})
        elif ext == 'json':
            return cls._expand_vars(Json.read(path) or {})
        else:
            raise UnrecognizedManifestFileFormat(
                f'only json and yaml manifest files are '
                f'supported: {path}'
            )

    @classmethod
    def inherit_base_manifest(
        cls,
        base: Text = None,
        visited: Set = None,
        data: Dict = None,
    ):
        """
        If there is a base manifest file specified by the loaded manifest
        data, then load it here and merge the current manifest into the base
        manifest, recusively back to the root base.
        """
        visited = set() if visited is None else visited
        data = data if data is not None else {}
        base = os.path.abspath(
            os.path.expandvars(
                os.path.expanduser(base)
            )
        )

        if base in visited:
            # avoid infinite loop if there is a bad inheritence cycle
            raise ManifestInheritanceError(
                f'manifest inheritance loop detected: {base}'
            )
        else:
            visited.add(base)

        console.debug(
            f'inheriting manifest from {base}'
        )

        if os.path.isfile(base):
            # merge current data dict into new base dict
            base_data = cls._read_file(base)
            data = DictUtils.merge(base_data.copy(), data)
            # recurse on base's base manifest...
            nested_base = base_data.get('base')
            if nested_base:
                inherit_base_manifest(nested_base, visited, data)

        return data

    @classmethod
    def _expand_vars(cls, data: Dict) -> Dict:
        expanded = {}
        re_env_ident = re.compile(r'^\$[a-z]\w*$')
        for k, v in data.items():
            if isinstance(v, str) and re_env_ident.match(v, re.I):
                expanded[k] = os.environ.get(v.lstrip('$'))
            elif isinstance(v, dict):
                expanded[k] = cls._expand_vars(v)
            else:
                expanded[k] = v
        return expanded

    def _scan_filesystem(self) -> ManifestScanner:
        """
        Use venusian simply to scan the action packages/modules, causing the
        action callables to register themselves with the Application instance.
        """
        package = self.package
        pid = os.getpid()
        executor = ThreadPoolExecutor(
            max_workers=6,
            thread_name_prefix=(
                f'{StringUtils.camel(self.package or "")} '
                f'ManifestWorker (pid: {pid})'.strip()
            )
        )
        scanner = ManifestScanner()
        futures = []

        def scan(package):
            console.debug(f'manifest scanning {package}')
            scanner.scan(package)

        def async_scan(package):
            future = executor.submit(scan, package)
            futures.append(future)

        async_scan('ravel.resource')
        async_scan('ravel.store')

        installed_pkg_names = {
            pkg.key for pkg in pkg_resources.working_set
        }

        # scan extension directories if the package installation requirements
        # are met, like sqlalchemy and redis.
        pgk_name_2_ravel_scan_path = {
            'sqlalchemy': 'ravel.ext.sqlalchemy',
            'redis': 'ravel.ext.redis',
            'pygame': 'ravel.ext.gaming.pygame',
            'celery': 'ravel.ext.celery',
            'numpy': 'ravel.ext.np'
        }
        for pkg_name, scan_path in pgk_name_2_ravel_scan_path.items():
            if pkg_name in installed_pkg_names:
                async_scan(scan_path)

        # scan the app project package
        if package:
            async_scan(package)

        concurrent.futures.wait(futures)

        return scanner

    def _scan_namespace(self, namespace: Dict):
        """
        Non-recursively detect Store and Resource class objects contained in
        the given namespace dict, making them available to the bootstrapping
        app.
        """
        from ravel.store import Store
        from ravel import Resource

        for k, v in (namespace or {}).items():
            if isinstance(v, type):
                if issubclass(v, Resource) and v is not Resource:
                    if not v.ravel.is_abstract:
                        self.resource_classes[k] = v
                        console.debug(
                            f'detected Resource class in '
                            f'namespace dict: {get_class_name(v)}'
                        )
                elif issubclass(v, Store):
                    self.store_classes[k] = v
                    console.debug(
                        f'detected Store class in namespace '
                        f'dict: {get_class_name(v)}'
                    )


if __name__ == "__main__":
    import tempfile
    
    from ravel.util.json_encoder import JsonEncoder
    from ravel import Application

    json = JsonEncoder()
    data = {
        'package': 'ravel',
        'bootstraps': [
            {
                'store': 'SimulationStore',
                'default': True,
            }
        ],
        'values': {
            'env_var': '$HOME',
            'not_env_var': 'FOO $HOME',
        }
    }


    def test_manifest_from_dict():
        manifest = Manifest(data)
        assert manifest.data['package'] == 'ravel'
        assert manifest.filepath is None
        assert manifest.data['values']['env_var'] == manifest.env.HOME
        assert manifest.data['values']['not_env_var'] == 'FOO $HOME'


    def test_manifest_from_manifest():
        source_manifest = Manifest(data)
        manifest = Manifest(source_manifest)

        assert manifest.data['package'] == 'ravel'
        assert manifest.filepath is None


    def test_manifest_from_filepath():
        with tempfile.NamedTemporaryFile(suffix='.json') as manifest_file:
            manifest_file.write(json.encode(data).encode())
            manifest_file.flush()

            manifest = Manifest(manifest_file.name)

            assert manifest.filepath == manifest_file.name
            assert manifest.data is not None
            assert manifest.data['package'] == 'ravel'

    def test_manifest_from_filepath_with_base():
        global data
        data = data.copy()

        with tempfile.NamedTemporaryFile(suffix='.json') as manifest_file:
            with tempfile.NamedTemporaryFile(suffix='.json') as base_manifest_file:
                data['base'] = base_manifest_file.name

                base_manifest_file.write(
                    json.encode({'values': {'base': True}}).encode()
                )
                base_manifest_file.flush()

                manifest_file.write(json.encode(data).encode())
                manifest_file.flush()

                manifest = Manifest(manifest_file.name)

                assert manifest.filepath == manifest_file.name
                assert manifest.data is not None
                assert manifest.data['package'] == 'ravel'
                assert manifest.data['values'].get('base') is True

    def test_manifest_bootstrap_creates_bootstraps():
        global data
        data = data.copy()
        del data['package']

        app = Application()

        manifest = Manifest(data)
        manifest.bootstrap(app)

        assert len(manifest.bootstraps) == len(data['bootstraps'])


    #test_manifest_from_dict()
    #test_manifest_from_manifest()
    #test_manifest_from_filepath()
    # test_manifest_from_filepath_with_base()
    test_manifest_bootstrap_creates_bootstraps()