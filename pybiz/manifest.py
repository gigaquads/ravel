import os
import importlib
import re

import yaml

from typing import Text, Dict
from collections import defaultdict

from venusian import Scanner
from appyratus.memoize import memoized_property
from appyratus.utils import DictUtils, DictAccessor
from appyratus.files import Yaml, Json
from appyratus.env import Environment

from pybiz.exc import ManifestError


class Manifest(object):
    """
    At its base, a manifest file declares the name of an installed pybiz project
    and a list of bindings, relating each BizObject class defined in the project
    with a Dao class.
    """

    def __init__(self, path: Text = None, data: Dict = None, load=True):
        self.scanner = Scanner(
            bizobj_classes={},
            dao_classes={}
        )
        self.types = DictAccessor({
            'biz': self.scanner.bizobj_classes,
            'dao': self.scanner.dao_classes,
        })
        self.package = None
        self.bindings = []
        self.bootstraps = {}
        if data or path:
            self.load(data=data, path=path)

    def load(self, data: Dict = None, path: Text = None, env=None):
        if not (data or path):
            return

        data = data or {}

        # load base data from file
        if path is None:
            path = os.environ.get('PYBIZ_MANIFEST')
        if path is not None:
            _, ext = os.path.splitext(path)
            ext = ext.lstrip('.').lower()
            if ext in ('yml', 'yaml'):
                file_data = Yaml.load_file(path)
            elif ext == 'json':
                file_data = Json.load_file(path)
            # merge contents of file with data dict arg
            data = DictUtils.merge(file_data, data)

        self._expand_environment_vars(data, env)

        # marshal in the computed data dict
        self.package = data.get('package')

        for binding_data in data['bindings']:
            biz = binding_data['biz']
            dao = binding_data.get('dao', 'DictDao')
            params = binding_data.get('parameters', {})
            self.bindings.append(Binding(
                biz=biz, dao=dao, params=params,
            ))

        self.bootstraps = {}
        for record in data.get('bootstraps', []):
            self.bootstraps[record['dao']] = Bootstrap(
                dao=record['dao'],
                params=record.get('params', {})
            )

        return self

    def process(self, namespace: Dict = None, on_error=None):
        """
        Interpret the manifest file data, bootstrapping the layers of the
        framework.
        """
        self._discover_pybiz_types(namespace, on_error)
        self._bind_dao_to_bizobj_types()
        return self

    def _discover_pybiz_types(self, namespace, on_error):
        if self.package:
            self._scan_venusian(namespace=namespace, on_error=on_error)
        if namespace:
            self._scan_namespace(namespace)

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

    def _scan_venusian(self, namespace : Dict = None, on_error=None):
        """
        Use venusian simply to scan the endpoint packages/modules, causing the
        endpoint callables to register themselves with the Api instance.
        """
        if on_error is None:
            def on_error(name):
                import sys, re
                if issubclass(sys.exc_info()[0], ImportError):
                    # XXX add logging otherwise things
                    # like import errors do not surface
                    if re.match(r'^\w+\.grpc', name):
                        return

        pkg_path = self.package
        if pkg_path:
            pkg = importlib.import_module(pkg_path)
            self.scanner.scan(pkg, onerror=on_error)

        self.types.biz.update(self.scanner.biz_classes)
        self.types.dao.update(self.scanner.dao_classes)

    def _bind_dao_to_bizobj_types(self):
        """
        Associate each BizObject class with a corresponding Dao class. Also bind
        Schema classes to their respective BizObject classes.
        """
        from pybiz.dao.dict_dao import DictDao

        for binding in self.bindings:
            biz_class = self.scanner.bizobj_classes.get(binding.biz)
            if biz_class is None:
                raise ManifestError('{} not found'.format(binding.biz))

            dao_class = self.scanner.dao_classes.get(binding.dao)
            if dao_class is None and biz_class:
                print(f'Binding default DictDao to {biz_class.__name__}...')
                if not biz_class.dal.is_registered(biz_class):
                    biz_class.dal.register(
                        biz_class, DictDao, dao_kwargs=binding.params
                    )
                    continue

            if biz_class and dao_class:
                biz_class.dal.register(
                    biz_class, dao_class, dao_kwargs=binding.params
                )

    def _expand_environment_vars(self, data, env=None):
        re_env_var = re.compile(r'^\$([\w\-]+)$')
        env = env or Environment(allow_additional=True)

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
    def __init__(self, biz, dao, params=None):
        self.biz = biz
        self.dao = dao
        self.params = params


class Bootstrap(object):
    def __init__(self, dao: Text, params: Dict = None):
        self.dao = dao
        self.params = params or {}
