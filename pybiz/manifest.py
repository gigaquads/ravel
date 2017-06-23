import os
import inspect
import importlib

import yaml
import venusian

import pybiz.schema as fields

from pybiz.dao import DaoManager
from pybiz.schema import Schema


class ManifestSchema(Schema):
    """
    Describes the structure expected in manifest.yaml files.
    """

    class DataAccessMappingSchema(Schema):
        business_object = fields.Str(required=True)
        data_access_object = fields.Str(required=True)

    api_endpoints = fields.List(fields.Str(), required=True)
    data_access_layer = fields.List(DataAccessMappingSchema(), required=True)


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

    def __init__(self, api, filepath=None):
        self.api = api
        self.filepath = self._get_manifest_filepath(api, filepath)
        self.data = self._load_manifest_file()

    def process(self):
        """
        Interpret the manifest file data, bootstrapping the layers of the
        framework.
        """
        self._bind_data_access_objects_to_bizobjs()
        self._scan_endpoint_modules()

    @staticmethod
    def _get_manifest_filepath(api, filepath):
        """
        Return the filepath to the manifest.yaml file for self.api. Search the
        following places for the filepath if the filepath argument is None:

            1. Check for env var with the structure: {SERVICE_NAME}_MANIFEST
            2. Search the project dir for a manifest.yaml file.
        """
        # get manifest file path from environ var. The name of the var is
        # dynamic. if the service package is called my_service, then the
        # expected var name will be MY_SERVICE_MANIFEST
        if filepath is None:
            root_pkg_name = api.__class__.__module__.split('.')[0]
            env_var_name = '{}_MANIFEST'.format(root_pkg_name.upper())
            filepath = os.environ.get(env_var_name)

        # get project directory (the parent dir to src dir)
        if filepath is None:
            root_pkg = importlib.import_module(root_pkg_name)
            abs_root_dir = os.path.realpath(root_pkg.__file__)
            proj_dir = '/'.join(abs_root_dir.split('/')[:-2])
            filepath = os.path.join(proj_dir, 'manifest.yaml')

        return filepath

    def _load_manifest_file(self):
        with open(self.filepath) as manifest_file:
            schema = ManifestSchema()
            manifest = yaml.load(manifest_file)
            return schema.load(manifest, strict=True).data

    def _bind_data_access_objects_to_bizobjs(self):
        """
        Associate each BizObject class with a corresponding Dao class.
        """
        manager = DaoManager.get_instance()
        for item in self.data.get('data_access_layer', []):
            manager.register(
                self._import_object(item['business_object']),
                self._import_object(item['data_access_object']))

    def _scan_endpoint_modules(self):
        """
        Use venusian simply to scan the endpoint packages/modules, causing the
        endpoint callables to register themselves with the Api instance.
        """
        scanner = venusian.Scanner()
        endpoint_module_paths = self.data.get('api_endpoints')
        for path in endpoint_module_paths:
            obj = importlib.import_module(path)
            scanner.scan(obj)

    @staticmethod
    def _import_object(path_str):
        """
        Import an object from a module, given a dotted path to said object.
        """
        path = path_str.split('.')
        module_path, obj_name = '.'.join(path[:-1]), path[-1]
        module = importlib.import_module(module_path)
        return getattr(module, obj_name)
