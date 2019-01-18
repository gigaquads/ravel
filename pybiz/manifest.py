import os
import importlib

import yaml

from typing import Text, Dict

from venusian import Scanner
from appyratus.memoize import memoized_property
from appyratus.utils import DictUtils, DictAccessor
from appyratus.files import Yaml, Json

from pybiz.exc import ManifestError


class Manifest(object):
    """
    At its base, a manifest file declares the name of an installed pybiz project
    and a list of bindings, relating each BizObject class defined in the project
    with a Dao class.
    """

    def __init__(self, path: Text = None, data: Dict = None, load=True):
        self.scanner = Scanner(bizobj_classes={}, dao_classes={})
        self.types = DictAccessor({})
        self.package = None
        self.bindings = []

        self.load(data=data, path=path)

    def load(self, data: Dict = None, path: Text = None):
        self.package = None
        self.bindings = []

        if not (data or path):
            return

        data = {}

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

        # marshal in the computed data dict
        self.package = data['package']
        for binding_data in data['bindings']:
            biz = binding_data['biz']
            dao = binding_data['dao']
            params = binding_data.get('parameters', {})
            self.bindings.append(Binding(
                biz=biz, dao=dao, params=params,
            ))

        return self

    def process(self, namespace: Dict = None, on_error=None):
        """
        Interpret the manifest file data, bootstrapping the layers of the
        framework.
        """
        self._scan(namespace=namespace, on_error=on_error)
        self._bind()
        return self

    def _scan(self, namespace : Dict = None, on_error=None):
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
        else:
            # try to load whatever's in global namespace
            from pybiz.dao import Dao
            from pybiz.biz import BizObject

            for k, v in (namespace or {}).items():
                if isinstance(v, type):
                    if issubclass(v, BizObject):
                        self.scanner.bizobj_classes[k] = v
                    elif issubclass(v, Dao):
                        self.scanner.dao_classes[k] = v

        self.types = DictAccessor({
            'biz': self.scanner.bizobj_classes,
            'dao': self.scanner.dao_classes,
        })

    def _bind(self):
        """
        Associate each BizObject class with a corresponding Dao class. Also bind
        Schema classes to their respective BizObject classes.
        """
        for binding in self.bindings:
            biz_class = self.scanner.bizobj_classes.get(binding.biz)
            if biz_class is None:
                raise ManifestError('{} not found'.format(binding.biz))

            dao_class = self.scanner.dao_classes.get(binding.dao)
            if dao_class is None:
                raise ManifestError('{} not found'.format(binding['dao']))

            biz_class.dal.register(
                biz_class, dao_class, dao_kwargs=binding.params
            )


class Binding(object):
    def __init__(self, biz, dao, params=None):
        self.biz = biz
        self.dao = dao
        self.params = params

