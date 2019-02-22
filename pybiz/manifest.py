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
    ):
        from pybiz.dao import PythonDao

        self.data = data or {}
        self.path = path
        self.package = None
        self.bindings = []
        self.bootstraps = {}
        self.env = env or Environment()
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
        if not (self.data or self.path):
            return self

        # try to load manifest file from a YAML or JSON file
        if self.path is not None:
            ext = os.path.splitext(self.path)[1].lstrip('.').lower()
            if ext in Yaml.extensions():
                file_data = Yaml.load_file(self.path)
            elif ext in Json.extensions():
                file_data = Json.load_file(self.path)

            # merge contents of file with data dict arg
            self.data = DictUtils.merge(file_data, self.data)

        # replace env $var names with values from env
        self._expand_environment_vars(self.env, self.data)

        self.package = self.data.get('package')

        for binding_data in (self.data.get('bindings') or []):
            biz = binding_data['biz']
            dao = binding_data.get('dao', 'PythonDao')
            params = binding_data.get('params', {})
            self.bindings.append(Binding(
                biz=biz,
                dao=dao,
                params=params,
            ))

        # TODO: rename to something that means parameters to bootstrap methods
        self.bootstraps = {}
        for record in self.data.get('bootstraps', []):
            self.bootstraps[record['dao']] = Bootstrap(
                dao=record['dao'], params=record.get('params', {})
            )

        return self

    def process(
        self,
        namespace: Dict = None,
        binder: 'DaoBinder' = None,
        override=True,
        on_error=None,
    ):
        """
        Interpret the manifest file data, bootstrapping the layers of the
        framework.
        """
        self._discover_pybiz_types(namespace, override, on_error)
        self._register_dao_types(binder=binder, override=override)
        return self

    def _discover_pybiz_types(self, namespace: Dict, override: bool, on_error):
        if self.package:
            # package name for venusian scan
            self._scan_venusian(on_error=on_error)
        if namespace:
            # load BizObject and Dao classes from a namespace dict
            self._scan_namespace(namespace)

        # load BizObject and Dao classes from dotted path strings in bindings
        self._scan_dotted_paths(override)

        # the following ensures that each manifest gets distinct dao classes
        # so that processing multiple manifests in a single application
        # does not trample a singular Dao class namespace while bootstrapping.
        for k, v in self.types.dao.items():
            self.types.dao[k] = type(v.__name__, (v, ), {})

    def _register_dao_types(self, binder: 'DaoBinder' = None, override=True):
        """
        Associate each BizObject class with a corresponding Dao class.
        """
        from pybiz.biz import BizObject
        from pybiz.dao import DaoBinder

        binder = binder or DaoBinder.get_instance()
        visited_biz_types = set()

        for binding_spec in self.bindings:
            biz_type = self.types.biz.get(binding_spec.biz)
            dao_type = self.types.dao[binding_spec.dao]
            visited_biz_types.add(biz_type)
            if override or (not binder.is_registered(biz_type)):
                binding = binder.register(
                    biz_type=biz_type,
                    dao_type=dao_type,
                    dao_bind_kwargs=binding_spec.params,
                )
                self.types.dao[dao_type.__name__] = binding.dao_type

    def _scan_dotted_paths(self, override: bool):
        for binding in self.bindings:
            if binding.biz_module and (
                override or binding.biz not in self.types.biz
            ):
                biz_type = import_object(f'{binding.biz_module}.{binding.biz}')
                self.types.biz[binding.biz] = biz_type
            if binding.dao_module and (
                override or binding.dao not in self.types.dao
            ):
                dao_type = import_object(f'{binding.dao_module}.{binding.dao}')
                self.types.dao[binding.dao] = dao_type

    def _scan_namespace(self, namespace: Dict):
        """
        Populate self.types from namespace dict.
        """
        from pybiz.dao import Dao
        from pybiz.biz import BizObject

        for k, v in (namespace or {}).items():
            if isinstance(v, type):
                if issubclass(v, BizObject):
                    self.types.biz[k] = v
                elif issubclass(v, Dao):
                    self.types.dao[k] = v

    def _scan_venusian(self, on_error=None):
        """
        Use venusian simply to scan the endpoint packages/modules, causing the
        endpoint callables to register themselves with the Api instance.
        """
        if on_error is None:
            def on_error(name):
                import sys

                exc = sys.exc_info()[0]
                msg = traceback.format_exc().strip().split('\n')[-1]
                print(
                    f'Venusian ignoring {name}\n -> {msg}'
                )

        pkg_path = self.package
        if pkg_path:
            pkg = importlib.import_module(pkg_path)
            self.scanner.scan(pkg, onerror=on_error)

    @staticmethod
    def _expand_environment_vars(env, data):
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
                for k, v in data.items():
                    data[k] = expand(v)
                return data
            else:
                return data

        return expand(data)


class Binding(object):
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


class Bootstrap(object):
    def __init__(self, dao: Text, params: Dict = None):
        self.dao = dao
        self.params = params or {}
