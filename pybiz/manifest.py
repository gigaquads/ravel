import importlib
import os
import re
import sys
import traceback

import yaml

import pybiz

from typing import Text, Dict
from collections import defaultdict

from venusian import Scanner
from appyratus.memoize import memoized_property
from appyratus.utils import DictUtils, DictObject
from appyratus.files import Yaml, Json
from appyratus.env import Environment

from pybiz.exceptions import ManifestError
from pybiz.util.misc_functions import import_object, get_class_name
from pybiz.util.loggers import console


class Manifest(object):
    """
    At its base, a manifest file declares the name of an installed pybiz project
    and a list of bindings, relating each Resource class defined in the project
    with a Store class.
    """

    def __init__(
        self,
        path: Text = None,
        data: Dict = None,
        env: Environment = None,
    ):
        from pybiz.store import SimulationStore

        self.data = data or {}
        self.path = path
        self.app = None
        self.package = None
        self.bindings = []
        self._biz_2_store_name = {}
        self.bootstraps = {}
        self.env = env or Environment()
        self.types = DictObject({
            'dal': {
                'SimulationStore': SimulationStore
            },
            'biz': {},
        })
        self.scanner = Scanner(
            biz_classes=self.types.biz,
            store_classes=self.types.dal,
            env=self.env,
        )

    def load(self):
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
        base_data = self.data
        if not (self.data or self.path):
            return self

        # try to load manifest file from a YAML or JSON file
        if self.path is not None:
            console.debug(message='loading manifest file', data={'path': self.path})
            ext = os.path.splitext(self.path)[1].lstrip('.').lower()
            if ext in Yaml.extensions():
                file_data = Yaml.read(self.path)
            elif ext in Json.extensions():
                file_data = Json.read(self.path)
            else:
                file_data = {}

            # merge contents of file with data dict arg
            self.data = DictUtils.merge(file_data, self.data)

        if not self.data:
            self.data = {}

        # replace env $var names with values from env
        self._expand_environment_vars(self.env, self.data)

        console.debug(message='manifest loaded!', data={'manifest': self.data})

        self.package = self.data.get('package')

        if not self.data.get('bindings'):
            console.warning(f'no "bindings" section detected in manifest!')

        for binding_data in (self.data.get('bindings') or []):
            biz = binding_data['biz']
            store = binding_data.get('store', 'SimulationStore')
            params = binding_data.get('params', {})
            binding = ManifestBinding(biz=biz, store=store, params=params)
            self.bindings.append(binding)
            self._biz_2_store_name[biz] = store

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

    def process(self, app: 'Application', namespace: Dict = None):
        """
        Discover and prepare all Resource and Store classes for calling the
        bootstrap and bind lifecycle methods, according to the specification
        provided by the Manifest. If `namespace` is provided, self.process will
        include this contents of this dict in its scan for Resource and Store
        types.
        """
        self.app = app
        self._discover_pybiz_classes(namespace)
        self._register_store_classes()
        return self

    def bootstrap(self):
        # visited_store_classes is used to keep track of which DAO classes we've
        # already bootstrapped in the process of bootstrapping the DAO classes
        # bound to Resource classes.
        visited_store_classes = set()

        for biz_class in self.types.biz.values():
            if not (biz_class.pybiz.is_abstract
                    or biz_class.pybiz.is_bootstrapped):
                console.debug(
                    f'bootstrapping "{biz_class.__name__}" Resource...'
                )
                biz_class_name = get_class_name(biz_class)
                store_class_name = self._biz_2_store_name.get(biz_class_name)
                if store_class_name is None:
                    store_class = biz_class.__store__()
                    store_class_name = get_class_name(store_class)
                else:
                    assert store_class_name in self.types.dal
                    store_class = self.types.dal[store_class_name]

                biz_class.bootstrap(app=self.app)

                if store_class_name not in self.types.dal:
                    self.types.dal[store_class_name] = store_class

                if biz_class_name not in self._biz_2_store_name:
                    self._biz_2_store_name[biz_class_name] = store_class_name
                    self.bindings.append(
                        ManifestBinding(biz=biz_class_name, store=store_class_name)
                    )
                    self.app.binder.register(
                        biz_class=biz_class, store_class=store_class
                    )

                # don't bootstrap the store class if we've done so already
                if store_class in visited_store_classes:
                    continue
                else:
                    visited_store_classes.add(store_class)

                console.debug(f'bootstrapping "{store_class_name}" Store...')
                bootstrap_object = self.bootstraps.get(store_class_name)
                bootstrap_kwargs = bootstrap_object.params if bootstrap_object else {}
                store_class.bootstrap(app=self.app, **bootstrap_kwargs)

        console.debug(f'finished bootstrapped Store and Resource classes')

        # inject the following into each endpoint target's lexical scope:
        # all other endpoints, all Resource and Store classes.
        for endpoint in self.app.endpoints.values():
            endpoint.target.__globals__.update(self.types.biz)
            endpoint.target.__globals__.update(self.types.dal)
            endpoint.target.__globals__.update(
                {p.name: p.target
                 for p in self.app.endpoints.values()}
            )

    def bind(self, rebind=False):
        self.app.binder.bind(rebind=rebind)

    def _discover_pybiz_classes(self, namespace: Dict):
        # package name for venusian scan
        self._scan_venusian()

        if namespace:
            # load Resource and Store classes from a namespace dict
            self._scan_namespace(namespace)

        # load Resource and Store classes from dotted path strings in bindings
        self._scan_dotted_paths()

        # remove base Resource class from types dict
        self.types.biz.pop('Resource', None)
        self.types.dal.pop('Store', None)

    def _register_store_classes(self):
        """
        Associate each Resource class with a corresponding Store class.
        """
        # register each binding declared in the manifest with the ResourceBinder
        for info in self.bindings:
            biz_class = self.types.biz.get(info.biz)
            if biz_class is None:
                raise ManifestError(
                    f'cannot register {info.biz} with ResourceBinder because '
                    f'the class was not found while processing the manifest'
                )
            store_class = self.types.dal[info.store]
            if not self.app.binder.is_registered(biz_class):
                binding = self.app.binder.register(
                    biz_class=biz_class,
                    store_class=store_class,
                    store_bind_kwargs=info.params,
                )
                self.types.dal[info.store] = binding.store_class

        # register all store types *not* currently declared in a binding
        # with the ResourceBinder.
        for type_name, store_class in self.types.dal.items():
            if not self.app.binder.get_store_class(type_name):
                self.app.binder.register(None, store_class)
                registered_store_class = self.app.binder.get_store_class(type_name)
                self.types.dal[type_name] = registered_store_class

    def _scan_dotted_paths(self):
        # gather Store and Resource types in "bindings" section
        # into self.types.dal and self.types.biz
        for binding in self.bindings:
            if binding.biz_module and binding.biz not in self.types.biz:
                biz_class = import_object(f'{binding.biz_module}.{binding.biz}')
                self.types.biz[binding.biz] = biz_class
            if binding.store_module and binding.store not in self.types.dal:
                store_class = import_object(f'{binding.store_module}.{binding.store}')
                self.types.dal[binding.store] = store_class

        # gather Store types in "bootstraps" section into self.types.dal
        for store_class_name, bootstrap in self.bootstraps.items():
            if '.' in bootstrap.store:
                store_class_path = bootstrap.store
                if store_class_name not in self.types.dal:
                    store_class = import_object(store_class_path)
                    self.types.dal[store_class_name] = store_class
            elif bootstrap.store not in self.types.dal:
                raise ManifestError(f'{bootstrap.store} not found')

    def _scan_namespace(self, namespace: Dict):
        """
        Populate self.types from namespace dict.
        """
        from pybiz.store import Store
        from pybiz import Resource
        from pybiz.biz2 import Resource

        for k, v in (namespace or {}).items():
            if isinstance(v, type):
                if issubclass(v, (Resource, Resource)) and v is not Resource:
                    if not v.pybiz.is_abstract:
                        self.types.biz[k] = v
                        console.debug(
                            f'detected Resource class in '
                            f'namespace dict: {v.__name__}'
                        )
                elif issubclass(v, Store):
                    self.types.dal[k] = v
                    console.debug(
                        f'detected Store class in namespace '
                        f'dict: {v.__name__}'
                    )

    def _scan_venusian(self):
        """
        Use venusian simply to scan the endpoint packages/modules, causing the
        endpoint callables to register themselves with the Application instance.
        """
        import pybiz.store
        import pybiz.contrib

        def on_error(name):
            from pybiz.util.loggers import console

            exc_str = traceback.format_exc()
            console.debug(
                message=f'venusian scan failed for {name}',
                data={'trace': exc_str.split('\n')}
            )

        console.debug('venusian scan for BizType and Store types initiated')

        self.scanner.scan(pybiz.store, onerror=on_error)
        self.scanner.scan(pybiz.contrib, onerror=on_error)

        pkg_path = self.package
        if pkg_path:
            pkg = importlib.import_module(pkg_path)
            self.scanner.scan(pkg, onerror=on_error)

    @staticmethod
    def _expand_environment_vars(env, data):
        """
        Replace all environment variables used as keys or values in the manifest
        data dict. These are string like `$my_env_var`.
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
        biz: Text,
        store: Text,
        params: Dict = None,
    ):
        self.store = store
        self.params = params

        if '.' in biz:
            self.biz_module, self.biz = os.path.splitext(biz)
            self.biz = self.biz[1:]
        else:
            self.biz_module, self.biz = None, biz

        if '.' in store:
            self.store_module, self.store = os.path.splitext(store)
            self.store = self.store[1:]
        else:
            self.store_module, self.store = None, store

    def __repr__(self):
        return f'<ManifestBinding({self.biz}, {self.store})>'


class ManifestBootstrap(object):
    def __init__(self, store: Text, params: Dict = None):
        self.store = store
        self.params = params or {}
