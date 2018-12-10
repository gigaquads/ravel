import os
import inspect
import importlib

import yaml
import venusian

from typing import Text, Dict

from appyratus.schema import fields, Schema
from appyratus.memoize import memoized_property
from appyratus.utils import DictUtils, DictAccessor
from appyratus.files import Yaml, Json

from pybiz.dao.base import DaoManager
from pybiz.exc import ManifestError


class Manifest(object):
    """
    This thing reads manifest.yaml files and bootstraps each layer of the
    framework, namely:

        1. Associate each listed BizObject class with the Dao class it is
           with which it is associated.

        2. Do a venusian scan on the api endpoint packages and modules, which
           has the side-effect of registering the api callables with the
           ApiRegistry via ApiRegistryDecorators.
    """

    class Schema(Schema):
        """
        Describes the structure expected in manifest.yaml files.
        """

        class BindingSchema(Schema):
            biz = fields.String(required=True)
            dao = fields.String(required=True)

        package = fields.String()
        bindings = fields.List(BindingSchema(), default=lambda: [])


    def __init__(self, path: Text = None, data: Dict = None):
        self.data = {}
        self.schema = self.Schema()
        self.load(data=data, path=path)
        self.scanner = venusian.Scanner(
            bizobj_classes={},
            dao_classes={},
        )

    def load(self, data: Dict = None, path: Text = None):
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

        # marshal in the computed data dict
        self.data = DictUtils.merge(self.data, data)
        self.data, errors = self.schema.process(self.data)
        if errors:
            raise ManifestError(str(errors))

        return self

    def process(self, namespace: Dict = None, on_error=None):
        """
        Interpret the manifest file data, bootstrapping the layers of the
        framework.
        """
        self._scan(namespace=namespace, on_error=on_error)
        self._bind()

        return self

    @property
    def package(self):
        return self.data.get('package', None)

    @property
    def bindings(self):
        return self.data.get('bindings', [])

    @memoized_property
    def types(self) -> DictAccessor:
        return self._types

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

        pkg_path = self.data.get('package')
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

        self._types = DictAccessor({
            'biz': self.scanner.bizobj_classes,
            'dao': self.scanner.dao_classes,
        })

    def _bind(self):
        """
        Associate each BizObject class with a corresponding Dao class. Also bind
        Schema classes to their respective BizObject classes.
        """
        for binding in (self.data.get('bindings') or []):
            biz_class = self.scanner.bizobj_classes.get(binding['biz'])
            if biz_class is None:
                raise ManifestError('{} not found'.format(binding['biz']))

            dao_class = self.scanner.dao_classes.get(binding['dao'])
            if dao_class is None:
                raise ManifestError('{} not found'.format(binding['dao']))

            manager = DaoManager.get_instance()
            manager.register(biz_class, dao_class)
