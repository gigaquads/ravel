import os
import inspect
import importlib

import yaml
import venusian

from typing import Text, Dict

from appyratus.schema import fields, Schema
from appyratus.memoize import memoized_property
from appyratus.utils import DictUtils, DictAccessor
from appyratus.files import Yaml

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
        self.data = data or {}
        self.schema = self.Schema()
        self.scanner = venusian.Scanner(
            bizobj_classes={},
            schema_classes={},
            dao_classes={},
        )
        # load and merge contents of YAML file with data dict arg
        # if a file path to a manfiest file exists.
        if path is None:
            path = os.environ.get('PYBIZ_MANIFEST')
        if path is not None:
            yaml_data = Yaml.load_file(path)
            self.data = DictUtils.merge(yaml_data, self.data)

        # marshal in the computed data dict
        self.data, errors = self.schema.process(self.data)
        if errors:
            raise ManifestError(str(errors))

    def process(self, on_error=None):
        """
        Interpret the manifest file data, bootstrapping the layers of the
        framework.
        """
        self._scan(on_error=on_error)
        self._bind()
        return self

    @property
    def package(self):
        return self.data['package']

    @memoized_property
    def biz_types(self) -> DictAccessor:
        return DictAccessor(self.scanner.bizobj_classes)

    @memoized_property
    def dao_types(self) -> DictAccessor:
        return DictAccessor(self.scanner.dao_classes)

    @memoized_property
    def schemas(self) -> DictAccessor:
        return DictAccessor(self.scanner.schema_classes)

    def _scan(self, on_error=None):
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
