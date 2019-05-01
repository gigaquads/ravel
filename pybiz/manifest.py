import importlib
import os
import re
import sys
import traceback

import yaml

from typing import Text, Dict
from collections import defaultdict

from venusian import Scanner
from appyratus.memoize import memoized_property
from appyratus.utils import DictUtils, DictObject
from appyratus.files import Yaml, Json
from appyratus.env import Environment

from pybiz.exc import ManifestError
from pybiz.util import import_object
from pybiz.util.loggers import console
from pybiz.dao import DaoBinder


class Manifest(object):
    """
    At its base, a manifest file declares the name of an installed pybiz project
    and a list of bindings, relating each BizObject class defined in the project
    with a Dao class.
    """

    def __init__(
        self,
        path: Text = None,
        data: Dict = None,
        env: Environment = None,
        binder: DaoBinder = None,
    ):
        from pybiz.dao import PythonDao

        self.data = data or {}
        self.path = path
        self.package = None
        self.bindings = []
        self.bootstraps = {}
        self.env = env or Environment()
        self.binder = binder or DaoBinder.get_instance()
        self.types = DictObject({
            'dao': {'PythonDao': PythonDao},
            'biz': {},
        })
        self.scanner = Scanner(
            biz_types=self.types.biz,
            dao_types=self.types.dao,
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
        if not (self.data or self.path):
            return self

        # try to load manifest file from a YAML or JSON file
        if self.path is not None:
            console.debug(
                message='loading manifest file',
                data={'path': self.path}
            )
            ext = os.path.splitext(self.path)[1].lstrip('.').lower()
            if ext in Yaml.extensions():
                file_data = Yaml.load_file(self.path)
            elif ext in Json.extensions():
                file_data = Json.load_file(self.path)

            # merge contents of file with data dict arg
            self.data = DictUtils.merge(file_data, self.data)

        if not self.data:
            self.data = {}

        # replace env $var names with values from env
        self._expand_environment_vars(self.env, self.data)

        console.info('manifest', data=self.data)

        self.package = self.data.get('package')

        for binding_data in (self.data.get('bindings') or []):
            biz = binding_data['biz']
            dao = binding_data.get('dao', 'PythonDao')
            params = binding_data.get('params', {})
            binding = ManifestBinding(
                biz=biz,
                dao=dao,
                params=params,
            )
            self.bindings.append(binding)

        # create self.bootstraps
        self.bootstraps = {}
        for record in self.data.get('bootstraps', []):
            if '.' in record['dao']:
                dao_type_name = os.path.splitext(record['dao'])[-1][1:]
            else:
                dao_type_name = record['dao']
            self.bootstraps[dao_type_name] = ManifestBootstrap(
                dao=record['dao'], params=record.get('params', {})
            )

        return self

    def process(
        self,
        namespace: Dict = None,
    ):
        """
        Discover and prepare all BizObject and Dao classes for calling the
        bootstrap and bind lifecycle methods, according to the specification
        provided by the Manifest. If `namespace` is provided, self.process will
        include this contents of this dict in its scan for BizObject and Dao
        types.
        """
        self._discover_pybiz_types(namespace)
        self._register_dao_types()
        return self

    def bootstrap(self, registry: 'Registry'):
        for type_name, dao_type in self.types.dao.items():
            strap = self.bootstraps.get(type_name)
            if strap is not None:
                console.debug(
                    f'bootstrapping Dao type: {dao_type.__name__}'
                )
                dao_type.bootstrap(registry=registry, **strap.params)
        for biz_type in self.types.biz.values():
            if not (biz_type.is_abstract or biz_type.is_bootstrapped):
                console.debug(
                    f'bootstrapping BizObject type: {biz_type.__name__}'
                )
                biz_type.bootstrap(registry=registry)

        # inject all proxy target callables into each other's global namespace
        # to avoid having to import them across modules.
        targets = {p.name: p.target for p in registry.proxies.values()}
        for proxy in registry.proxies.values():
            proxy.target.__globals__.update(targets)

    def bind(self):
        self.binder.bind()

    def _discover_pybiz_types(self, namespace: Dict):
        # package name for venusian scan
        self._scan_venusian()
        if namespace:
            # load BizObject and Dao classes from a namespace dict
            self._scan_namespace(namespace)

        # load BizObject and Dao classes from dotted path strings in bindings
        self._scan_dotted_paths()

        # remove base BizObject class from types dict
        self.types.biz.pop('BizObject', None)
        self.types.dao.pop('Dao', None)

    def _register_dao_types(self):
        """
        Associate each BizObject class with a corresponding Dao class.
        """
        # register each binding declared in the manifest with the DaoBinder
        for info in self.bindings:
            biz_type = self.types.biz.get(info.biz)
            if biz_type is None:
                raise ManifestError(
                    f'cannot register {info.biz} with DaoBinder because '
                    f'the class was not found while processing the manifest'
                )
            dao_type = self.types.dao[info.dao]
            if not self.binder.is_registered(biz_type):
                binding = self.binder.register(
                    biz_type=biz_type,
                    dao_type=dao_type,
                    dao_bind_kwargs=info.params,
                )
                self.types.dao[info.dao] = binding.dao_type

        # register all dao types *not* currently declared in a binding
        # with the DaoBinder.
        for type_name, dao_type in self.types.dao.items():
            if not self.binder.get_dao_type(type_name):
                self.binder.register(None, dao_type)
                self.types.dao[type_name] = self.binder.get_dao_type(type_name)

    def _scan_dotted_paths(self):
        # gather Dao and BizObject types in "bindings" section
        # into self.types.dao and self.types.biz
        for binding in self.bindings:
            if binding.biz_module and binding.biz not in self.types.biz:
                biz_type = import_object(f'{binding.biz_module}.{binding.biz}')
                self.types.biz[binding.biz] = biz_type
            if binding.dao_module and binding.dao not in self.types.dao:
                dao_type = import_object(f'{binding.dao_module}.{binding.dao}')
                self.types.dao[binding.dao] = dao_type

        # gather Dao types in "bootstraps" section into self.types.dao
        for dao_type_name, bootstrap in self.bootstraps.items():
            if '.' in bootstrap.dao:
                dao_type_path = bootstrap.dao
                if dao_type_name not in self.types.dao:
                    dao_type = import_object(dao_type_path)
                    self.types.dao[dao_type_name] = dao_type
            elif bootstrap.dao not in self.types.dao:
                raise ManifestError(f'{bootstrap.dao} not found')

    def _scan_namespace(self, namespace: Dict):
        """
        Populate self.types from namespace dict.
        """
        from pybiz.dao import Dao
        from pybiz.biz import BizObject

        for k, v in (namespace or {}).items():
            if isinstance(v, type):
                if issubclass(v, BizObject) and v is not BizObject:
                    self.types.biz[k] = v
                    console.debug(
                        f'detected BizObject type in '
                        f'namespace dict: {v.__name__}'
                    )
                elif issubclass(v, Dao):
                    self.types.dao[k] = v
                    console.debug(
                        f'detected Dao type in namespace '
                        f'dict: {v.__name__}'
                    )

    def _scan_venusian(self):
        """
        Use venusian simply to scan the endpoint packages/modules, causing the
        endpoint callables to register themselves with the Api instance.
        """
        import pybiz.dao
        import pybiz.contrib

        def on_error(name):
            from pybiz.util.loggers import console
            
            exc_str = traceback.format_exc()
            console.debug(
                message=f'venusian scan failed for {name}',
                data={
                    'trace': exc_str.split('\n')
                }
            )

        console.debug('venusian scan for BizType and Dao types initiated')

        self.scanner.scan(pybiz.dao, onerror=on_error)
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
        dao: Text,
        params: Dict = None,
    ):
        self.dao = dao
        self.params = params

        if '.' in biz:
            self.biz_module, self.biz = os.path.splitext(biz)
            self.biz = self.biz[1:]
        else:
            self.biz_module, self.biz = None, biz

        if '.' in dao:
            self.dao_module, self.dao = os.path.splitext(dao)
            self.dao = self.dao[1:]
        else:
            self.dao_module, self.dao = None, dao

    def __repr__(self):
        return f'<ManifestBinding({self.biz}, {self.dao})>'


class ManifestBootstrap(object):
    def __init__(self, dao: Text, params: Dict = None):
        self.dao = dao
        self.params = params or {}
