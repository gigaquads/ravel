import os
import inspect
import importlib
import yaml
import venusian

import pybiz.schema as fields

from abc import ABCMeta, abstractmethod
from collections import defaultdict

from pybiz.dao import DaoManager
from pybiz.schema import Schema
from pybiz.manifest import Manifest

from .const import (
    HTTP_GET,
    HTTP_POST,
    HTTP_PUT,
    HTTP_PATCH,
    HTTP_DELETE,
    )


class ApiRegistry(object, metaclass=ABCMeta):

    def __init__(self, hook=None, unpack=None, manifest_filepath: str=None):
        self.handlers = defaultdict(dict)
        self.hook = hook
        self.unpack = unpack
        self.manifest = None
        if manifest_filepath:
            self.manifest = Manifest(self, filepath=manifest_filepath)

    @abstractmethod
    def get_request(self, *args, **kwargs):
        """
        This method defines how the request object is extracted from the args or
        kwargs received by registered handlers (before unpacking) from your web
        framework. Override this method in a subclass.
        """

    @abstractmethod
    def get_response(self, *args, **kwargs):
        """
        This method defines how the response object is extracted from the args or
        kwargs received by registered handlers (before unpacking) from your web
        framework.
        """

    def get(self, path, schemas=None, hook=None, unpack=None):
        hook = hook or self.hook
        unpack = unpack or self.unpack
        return ApiRegistryDecorator(self, HTTP_GET, path,
            schemas=schemas, hook=hook, unpack=unpack)

    def post(self, path, schemas=None, hook=None, unpack=None):
        hook = hook or self.hook
        unpack = unpack or self.unpack
        return ApiRegistryDecorator(self, HTTP_POST, path,
            schemas=schemas, hook=hook, unpack=unpack)

    def put(self, path, schemas=None, hook=None, unpack=None):
        hook = hook or self.hook
        unpack = unpack or self.unpack
        return ApiRegistryDecorator(self, HTTP_PUT, path,
            schemas=schemas, hook=hook, unpack=unpack)

    def patch(self, path, schemas=None, hook=None, unpack=None):
        hook = hook or self.hook
        unpack = unpack or self.unpack
        return ApiRegistryDecorator(self, HTTP_PATCH, path,
            schemas=schemas, hook=hook, unpack=unpack)

    def delete(self, path, schemas=None, hook=None, unpack=None):
        hook = hook or self.hook
        unpack = unpack or self.unpack
        return ApiRegistryDecorator(self, HTTP_DELETE, path,
            schemas=schemas, hook=hook, unpack=unpack)

    def route(self, http_method, path, handler_args=None, handler_kwargs=None):
        handler = self.handlers[path.lower()][http_method.lower()]
        handler_args = handler_args or tuple()
        handler_kwargs = handler_kwargs or dict()
        return handler(*handler_args, **handler_kwargs)

    def bootstrap(self, filepath=None):
        """
        Bootstrap the data, business, and service layers, wiring them up,
        according to the settings contained in a service manifest file.
        """
        if self.manifest is not None:
            self.manifest.process()

    def validate_request(self, request, schema):
        pass

    def validate_params(self, request, schema):
        pass

    def validate_response(self, response, result, schema):
        pass


class ApiRegistryDecorator(object):

    def __init__(self,
            registry,
            http_method: str,
            path: str,
            schemas: dict = None,
            hook=None,
            unpack=None,
            ):

        self.registry = registry
        self.http_method = http_method.lower()
        self.path = path.lower()
        self.schemas = schemas or {}
        self.hook = hook
        self.unpack = unpack

    def __call__(self, func):
        handler = ApiHandler(func, self)
        if self.hook is not None:
            self.hook(handler)
        self.registry.handlers[self.path][self.http_method] = handler
        return handler


class ApiHandler(object):

    def __init__(self, target, decorator):
        self.target = target
        self.signature = inspect.signature(self.target)
        self.decorator = decorator

    def __repr__(self):
        return '<ApiHandler({})>'.format(', '.join([
                'method={}'.format(self.decorator.http_method.upper()),
                'path={}'.format(self.decorator.path),
                ]))

    def __call__(self, *args, **kwargs):
        registry = self.decorator.registry
        request = registry.get_request(*args, **kwargs)
        response = registry.get_response(*args, **kwargs)

        request_schema = self.decorator.schemas.get('request')
        if request_schema is not None:
            registry.validate_request(request, request_schema)

        params_schema = self.decorator.schemas.get('params')
        if params_schema is not None:
            registry.validate_params(request, params_schema)

        if self.decorator.unpack:
            # call the unpack to replace normal args to handler (namely request,
            # response, ...) with args and kwargs as if the handler is an RPC
            # method.
            args_dict = self.decorator.unpack(self.signature, *args, **kwargs)
            result = self.target(**args_dict)
        else:
            result = self.target(*args, **kwargs)

        response_schema = self.decorator.schemas.get('response')
        if response_schema is not None:
            registry.validate_response(response, result, response_schema)

        return result

    @property
    def http_method(self):
        return self.decorator.http_method

    @property
    def path(self):
        return self.decorator.path
