import os
import re
import traceback
import pkg_resources

from typing import Text, Dict, Type

from appyratus.utils import DictUtils, DictObject
from appyratus.files import Yaml, Json
from appyratus.env import Environment

from ravel.exceptions import ManifestError
from ravel.util.misc_functions import import_object, get_class_name
from ravel.util.loggers import console
from ravel.util.scanner import Scanner


class TypeScanner(Scanner):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        from ravel.store.base.store import Store
        from ravel.resource import Resource
        from ravel.app.base.action import Action

        self.Store = Store
        self.Resource = Resource
        self.Action = Action

    def predicate(self, value) -> bool:
        return (
            isinstance(value, type) and
            issubclass(value, (self.Action, self.Resource, self.Store))
        )

    def on_match(self, name, value, context):
        if issubclass(value, self.Action):
            context.api[name] = value
        elif issubclass(value, self.Resource) and not value.ravel.is_abstract:
            context.res[name] = value
        elif issubclass(value, self.Store):
            context.stores[name] = value

    def on_import_error(self, exc, module_name, context):
        exc_str = traceback.format_exc()
        console.error(
            message=f'could not scan module {module_name}',
            data={'trace': exc_str.split('\n')}
        )

    def on_match_error(self, exc, module, context, name, value):
        exc_str = traceback.format_exc()
        console.warning(
            message=f'error scanning {name} ({type(value)})',
            data={'trace': exc_str.split('\n')}
        )


class Manifest(object):
    """
    At its base, a manifest file declares the name of an installed ravel project
    and a list of bindings, relating each Resource class defined in the project
    with a Store class.
    """

    def __init__(
        self,
        path: Text = None,
        data: Dict = None,
        env: Environment = None,
        load: bool = True
    ):
        self.data = data or {}
        self.path = path
        self.app = None
        self.package = None
        self.bindings = []
        self.values = {}
        self._res_2_store_name = {}
        self.bootstraps = {}
        self.default_store_type_name = 'SimulationStore'
        self.env = env or Environment()
        self.types = DictObject({'stores': {}, 'res': {}, 'api': {}})
        self.scanner = TypeScanner(context=self.types)
        self._installed_pkg_names = {
            pkg.key for pkg in pkg_resources.working_set
        }

        if load:
            self.load()

    def __getitem__(self, key):
        return self.data[key]

    def __setitem__(self, key, value):
        self.data[key] = value

    @classmethod
    def from_object(cls, obj) -> 'Manifest':
        if obj is None:
            manifest = Manifest()
        elif isinstance(obj, Manifest):
            manifest = obj
        elif isinstance(obj, str):
            manifest = Manifest(path=obj)
        elif isinstance(obj, dict):
            manifest = Manifest(data=obj)
        else:
            manifest = manifest
        return manifest

    def get(self, key, default=None):
        return self.data.get(key, default)

    def load(self, values_schema: 'Schema' = None):
        """
        Load and merge manifest data from all supplied sources into a single
        dict, and then create internal Manifest data structures that hold this
        data.

        Data is merged in the following order:
            1. Data supplied by the manifest YAML/JSON file
            2. Data supplied by the data __init__ kwarg

        This process prepares:
            1. self.package
            2. self.bindings
            3. self.bootstraps
        """
        file_data = {}
        if not (self.data or self.path):
            return self

        # try to load manifest file from a YAML or JSON file
        if self.path is not None:
            if not os.path.isfile(self.path):
                raise OSError(f'manifest {self.path} does not exist')
            else:
                console.debug(f'loading manifest {self.path}...')
                file_data = self.read_file_data(self.path)

            # merge contents of file with data dict arg
            self.data = DictUtils.merge(file_data, self.data)

        if not self.data:
            self.data = {}

        self._inherit_base_manifest()

        # replace env $var names with values from env
        self._expand_environment_vars(self.env, self.data)

        if values_schema is not None:
            values = self.data.get('values') or {}
            self.values, errors = values_schema.process(values)
            if errors:
                raise ValueError(f'values validation error: {errors}')
        else:
            self.values = self.data.get('values') or {}

        console.debug(message='loaded manifest', data={'manifest': self.data})

        self.package = self.data.get('package')

        if not self.data.get('bindings'):
            console.warning('no "bindings" section detected in manifest!')

        for v in self.data.get('bootstraps', []):
            if v.get('default', False):
                self.default_store_type_name = v['store']
                break

        for binding_data in (self.data.get('bindings') or []):
            res = binding_data['resource']
            store_name = binding_data.get(
                'store', self.default_store_type_name
            )
            params = binding_data.get('params', {})
            binding = ManifestBinding(
                res=res, store=store_name, params=params
            )
            self.bindings.append(binding)
            self._res_2_store_name[res] = store_name

        # create self.bootstraps
        self.bootstraps = {}
        for record in self.data.get('bootstraps', []):
            if '.' in record['store']:
                store_class_name = os.path.splitext(record['store'])[-1][1:]
            else:
                store_class_name = record['store']
            self.bootstraps[store_class_name] = ManifestBootstrap(
                store=record['store'], params=record.get('params', {})
            )

        return self


    @staticmethod
    def read_file_data(path) -> Dict:
        ext = os.path.splitext(path)[1].lstrip('.').lower()
        if ext in Yaml.extensions():
            return Yaml.read(path)
        elif ext in Json.extensions():
            return Json.read(path)
        else:
            raise Exception('unknown manifest file format')

    def _inherit_base_manifest(self, base_file_path = None, visited = None):
        """
        If there is a base manifest file specified by the loaded manifest
        data, then load it here and merge the current manifest into the base
        manifest, recusively back to the root base.
        """
        visited = set() if visited is None else visited
        if self.path is not None:
            base_manifest_path = base_file_path or self.data.get('base')
            if base_manifest_path:
                base_manifest_path = os.path.realpath(base_manifest_path)
                if base_file_path in visited:
                    # avoid infinite loop if there is a bad inheritence cycle
                    raise ValueError('manifest inheritance loop detected')
                console.debug(f'inheriting manifest {base_manifest_path}...')
                visited.add(base_file_path)
                if os.path.exists(base_manifest_path):
                    base_data = self.read_file_data(base_manifest_path)
                    if base_data:
                        # merge and recurse up the tree...
                        self.data = DictUtils.merge(base_data.copy(), self.data)
                        if base_data.get('base'):
                            self._inherit_base_manifest(base_data['base'])

    def process(self, app: 'Application', namespace: Dict = None):
        """
        Discover and prepare all Resource and Store classes for calling the
        bootstrap and bind lifecycle methods, according to the specification
        provided by the Manifest. If `namespace` is provided, self.process will
        include this contents of this dict in its scan for Resource and Store
        types.
        """
        self.app = app
        self._discover_ravel_types(namespace)
        self._register_store_types()
        return self

    def bootstrap(self):
        # visited_store_types is used to keep track of which store classes
        # we've already bootstrapped in the process of bootstrapping the store
        # classes bound to Resource classes.
        visited_store_types = set()

        for resource_type in self.types.res.values():
            if not (
                resource_type.ravel.is_abstract
                or resource_type.ravel.is_bootstrapped
            ):
                console.debug(
                    f'bootstrapping {get_class_name(resource_type)} class'
                )

                resource_class_name = get_class_name(resource_type)
                store_class_name = self._res_2_store_name.get(
                    resource_class_name
                )

                if store_class_name is None:
                    store_type = resource_type.__store__()
                    store_class_name = get_class_name(store_type)
                else:
                    assert store_class_name in self.types.stores
                    store_type = self.types.stores[store_class_name]

                resource_type.bootstrap(app=self.app)

                if store_class_name not in self.types.stores:
                    self.types.stores[store_class_name] = store_type

                if resource_class_name not in self._res_2_store_name:
                    self._res_2_store_name[resource_class_name] = (
                        store_class_name
                    )
                    self.bindings.append(
                        ManifestBinding(
                            res=resource_class_name,
                            store=store_class_name
                        )
                    )
                    self.app.binder.register(
                        resource_type=resource_type, store_type=store_type
                    )

                # don't bootstrap the store class if we've done so already
                if store_type in visited_store_types:
                    continue
                else:
                    visited_store_types.add(store_type)

                console.debug(f'bootstrapping {store_class_name} class')

                bootstrap_object = self.bootstraps.get(store_class_name)
                bootstrap_kwargs = (
                    bootstrap_object.params if bootstrap_object else {}
                )

                store_type.bootstrap(app=self.app, **bootstrap_kwargs)

        # inject the following into each action target's lexical scope:
        # all other actions, all Resource and Store classes.
        for action in self.app.actions.values():
            action.target.__globals__.update(self.types.res)
            action.target.__globals__.update(self.types.stores)
            action.target.__globals__.update(
                {p.name: p.target
                 for p in self.app.actions.values()}
            )

    def get_bootstrap_params(self, store_type: Type['Store']) -> Dict:
        """
        Return the bootstrap kwargs dict associated with the given store type.
        """
        store_type_name = get_class_name(store_type)
        bootstrap_object = self.bootstraps.get(store_type_name)
        return bootstrap_object.params if bootstrap_object else {}

    def bind(self, rebind=False):
        # TODO: do away with "rebind" madness. forgot why it's even there.
        self.app.binder.bind(rebind=rebind)

    def _discover_ravel_types(self, namespace: Dict):
        self._scan_filesystem()

        if namespace:
            # load Resource and Store classes from a namespace dict
            self._scan_namespace(namespace)

        # load Resource and Store classes from dotted path strings in bindings
        self._scan_dotted_paths()

        # remove base Resource class from types dict
        self.types.res.pop('Resource', None)
        self.types.stores.pop('Store', None)

    def _register_store_types(self):
        """
        Associate each Resource class with a corresponding Store class.
        """
        # register each binding declared in manifest with ResourceBinder
        for info in self.bindings:
            resource_type = self.types.res.get(info.res)
            if resource_type is None:
                raise ManifestError(
                    f'cannot register {info.res} with ResourceBinder because '
                    f'the class was not found while processing the manifest'
                )
            store_type = self.types.stores[info.store]
            if not self.app.binder.is_registered(resource_type):
                binding = self.app.binder.register(
                    resource_type=resource_type,
                    store_type=store_type,
                    store_bind_kwargs=info.params,
                )
                self.types.stores[info.store] = binding.store_type

        # register all store types *not* currently declared in a binding
        # with the ResourceBinder.
        for type_name, store_type in self.types.stores.items():
            if not self.app.binder.get_store_type(type_name):
                self.app.binder.register(None, store_type)
                registered_type = self.app.binder.get_store_type(type_name)
                self.types.stores[type_name] = registered_type

    def _scan_dotted_paths(self):
        # gather Store and Resource types in "bindings" section
        # into self.types.stores and self.types.res
        for binding in self.bindings:
            if binding.res_module and binding.res not in self.types.res:
                resource_type = import_object(
                    f'{binding.res_module}.{binding.res}'
                )
                self.types.res[binding.res] = resource_type
            if binding.store_module and binding.store not in self.types.stores:
                store_type = import_object(
                    f'{binding.store_module}.{binding.store}'
                )
                self.types.stores[binding.store] = store_type

        # gather Store types in "bootstraps" section into self.types.stores
        for store_class_name, bootstrap in self.bootstraps.items():
            if '.' in bootstrap.store:
                store_class_path = bootstrap.store
                if store_class_name not in self.types.stores:
                    store_type = import_object(store_class_path)
                    self.types.stores[store_class_name] = store_type
            elif bootstrap.store not in self.types.stores:
                raise ManifestError(f'{bootstrap.store} not found')

    def _scan_namespace(self, namespace: Dict):
        """
        Populate self.types from namespace dict.
        """
        from ravel.store import Store
        from ravel import Resource

        for k, v in (namespace or {}).items():
            if isinstance(v, type):
                if issubclass(v, (Resource, Resource)) and v is not Resource:
                    if not v.ravel.is_abstract:
                        self.types.res[k] = v
                        console.debug(
                            f'detected Resource class in '
                            f'namespace dict: {v.__name__}'
                        )
                elif issubclass(v, Store):
                    self.types.stores[k] = v
                    console.debug(
                        f'detected Store class in namespace '
                        f'dict: {v.__name__}'
                    )

    def _scan_filesystem(self):
        """
        Use venusian simply to scan the action packages/modules, causing the
        action callables to register themselves with the Application instance.
        """
        # scan base ravel store and resource classes
        console.debug('scanning ravel.store store types')
        self.scanner.scan('ravel.store')

        console.debug('scanning ravel.resource for resource types')
        self.scanner.scan('ravel.resource')

        # scan extension directories if the package installation requirements
        # are met, like sqlalchemy and redis.
        if 'sqlalchemy' in self._installed_pkg_names:
            self.scanner.scan('ravel.ext.sqlalchemy')
        if 'redis' in self._installed_pkg_names:
            self.scanner.scan('ravel.ext.redis')
        if 'pygame' in self._installed_pkg_names:
            self.scanner.scan('ravel.ext.gaming.pygame')
        if 'falcon' in self._installed_pkg_names:
            self.scanner.scan('ravel.ext.falcon')
        if 'celery' in self._installed_pkg_names:
            self.scanner.scan('ravel.ext.celery')

        # scan the app project package
        if self.package:
            self.scanner.scan(self.package)

    @staticmethod
    def _expand_environment_vars(env, data):
        """
        Replace all environment variables used as keys or values in the
        manifest data dict. These are string like `$my_env_var`.
        """
        re_env_var = re.compile(r'^\$([\w\-]+)$')

        def expand(data):
            if isinstance(data, str):
                match = re_env_var.match(data)
                if match:
                    var_name = match.groups()[0]
                    return env[var_name]
                else:
                    return data
            elif isinstance(data, list):
                return [expand(x) for x in data]
            elif isinstance(data, dict):
                for k, v in list(data.items()):
                    if isinstance(k, str):
                        match = re_env_var.match(k)
                        if match:
                            data.pop(k)
                            k_new = match.groups()[0]
                            data[k_new] = v
                    data[k] = expand(v)
                return data
            else:
                return data

        return expand(data)


class ManifestBinding(object):
    def __init__(
        self,
        res: Text,
        store: Text,
        params: Dict = None,
    ):
        self.store = store
        self.params = params

        if '.' in res:
            self.res_module, self.res = os.path.splitext(res)
            self.res = self.res[1:]
        else:
            self.res_module, self.res = None, res

        if '.' in store:
            self.store_module, self.store = os.path.splitext(store)
            self.store = self.store[1:]
        else:
            self.store_module, self.store = None, store

    def __repr__(self):
        return f'<ManifestBinding({self.res}, {self.store})>'


class ManifestBootstrap(object):
    def __init__(self, store: Text, params: Dict = None):
        self.store = store
        self.params = params or {}
