import os
import inspect
import importlib

import yaml
import venusian

from collections import defaultdict

from pybiz.dao import DaoManager

from .const import (
    HTTP_GET,
    HTTP_POST,
    HTTP_PUT,
    HTTP_PATCH,
    HTTP_DELETE,
    )

# TODO: Figure out how to associate db connections with dao classes

class ApiRegistry(object):

    def __init__(self):
        self.handlers = defaultdict(dict)

    def get(self, path, schemas=None, hook=None):
        return ApiRegistryDecorator(self, HTTP_GET, path,
            schemas=schemas, hook=hook)

    def post(self, path, schemas=None, hook=None):
        return ApiRegistryDecorator(self, HTTP_POST, path,
            schemas=schemas, hook=hook)

    def put(self, path, schemas=None, hook=None):
        return ApiRegistryDecorator(self, HTTP_PUT, path,
            schemas=schemas, hook=hook)

    def patch(self, path, schemas=None, hook=None):
        return ApiRegistryDecorator(self, HTTP_PATCH, path,
            schemas=schemas, hook=hook)

    def delete(self, path, schemas=None, hook=None):
        return ApiRegistryDecorator(self, HTTP_DELETE, path,
            schemas=schemas, hook=hook)

    def route(self, http_method, path, handler_args=None, handler_kwargs=None):
        handler = self.handlers[path.lower()][http_method.lower()]
        handler_args = handler_args or tuple()
        handler_kwargs = handler_kwargs or dict()
        return handler(*handler_args, **handler_kwargs)

    def process_manifest(self, manifest_filepath=None):
        """
        Bootstrap the data, business, and service layers, wiring them up,
        according to the settings contained in a service manifest file.
        """
        # get manifest file path from environ var. The name of the var is
        # dynamic. if the service package is called my_service, then the
        # expected var name will be MY_SERVICE_MANIFEST
        if manifest_filepath is None:
            root_pkg_name = self.__class__.__module__.split('.')[0].upper()
            manifest_filepath = os.environ['{}_MANIFEST'.format(root_pkg_name)]

        bootstrapr = ApiBootstrapper(manifest_filepath)
        bootstrapr.bootstrap()

    def validate_request(self, request, schema):
        pass

    def validate_params(self, request, schema):
        pass

    def validate_response(self, response, result, schema):
        pass


class ApiBootstrapper(object):

    def __init__(self, manifest_filepath):
        self.manifest = {}
        with open(manifest_filepath) as manifest_file:
            self.manifest = yaml.load(manifest_file)

    def bootstrap(self):
        self._bootstrap_data_access_layer()
        self._bootstrap_endpoints()

    def _bootstrap_data_access_layer(self):
        """
        Associate each BizObject class with a corresponding Dao class.
        """
        manager = DaoManager.get_instance()
        for item in self.manifest.get('data_access_layer', []):
            manager.register(
                self._import_object(item['business_object']),
                self._import_object(item['dao']))

    def _bootstrap_endpoints(self):
        """
        Use venusian simply to scan the endpoint packages/modules, causing the
        endpoint callables to register themselves with the Api instance.
        """
        scanner = venusian.Scanner()
        service_layer = self.manifest.get('service_layer')
        if service_layer:
            endpoint_module_paths = service_layer.get('endpoints')
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


class ApiRegistryDecorator(object):

    def __init__(self,
            registry,
            http_method: str,
            path: str,
            schemas: dict = None,
            hook=None):

        self.registry = registry
        self.http_method = http_method.lower()
        self.path = path.lower()
        self.schemas = schemas or {}
        self.hook = hook

    def __call__(self, func):
        handler = ApiHandler(func, self)
        if self.hook is not None:
            self.hook(handler)
        self.registry.handlers[self.path][self.http_method] = handler
        return handler


class ApiHandler(object):
    def __init__(self, target, decorator):
        self.target = target
        self.decorator = decorator

    def __repr__(self):
        return '<ApiHandler({})>'.format(', '.join([
                'method={}'.format(self.decorator.http_method.upper()),
                'path={}'.format(self.decorator.path),
                ]))

    def __call__(self, *args, **kwargs):
        request, response = args[:2]
        registry = self.decorator.registry

        request_schema = self.decorator.schemas.get('request')
        if request_schema is not None:
            registry.validate_request(request, request_schema)

        params_schema = self.decorator.schemas.get('params')
        if params_schema is not None:
            registry.validate_params(request, params_schema)

        result = self.target(*args, **kwargs)

        response_schema = self.decorator.schemas.get('response')
        if response_schema is not None:
            registry.validate_response(response, result, response_schema)

        response.body = result
        return result
