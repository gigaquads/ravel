from __future__ import absolute_import

import importlib
import inspect

import falcon
import venusian

from abc import abstractmethod
from inspect import Signature

from pybiz.api import ApiRegistry, ApiHandler
from pybiz.const import HTTP_GET, HTTP_POST, HTTP_PUT, HTTP_PATCH, HTTP_DELETE

from .resource import FalconResourceManager
from .request import Request



class Api(ApiRegistry):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        request_type = self.request_type
        if request_type is None:
            request_type = falcon.Request

        self._falcon_api = falcon.API(
            middleware=self.middleware,
            request_type=request_type,
            )

        self._scanner = venusian.Scanner()
        self._resource_manager = FalconResourceManager()
        self._resources = {}

    @property
    def middleware(self):
        return []

    @property
    def request_type(self):
        return Request

    def scan(self, package: str):
        pkg = importlib.import_module(package)
        self._scanner.scan(pkg)

    def __call__(self, environ, start_response):
        return self._falcon_api(environ, start_response)

    def get(self, path, schemas=None):
        return super(Api, self).get(
            path, schemas=schemas, hook=self.hook, unpack=self.unpack)

    def post(self, path, schemas=None):
        return super(Api, self).post(
            path, schemas=schemas, hook=self.hook, unpack=self.unpack)

    def patch(self, path, schemas=None):
        return super(Api, self).patch(
            path, schemas=schemas, hook=self.hook, unpack=self.unpack)

    def put(self, path, schemas=None):
        return super(Api, self).put(
            path, schemas=schemas, hook=self.hook, unpack=self.unpack)

    def delete(self, path, schemas=None):
        return super(Api, self).delete(
            path, schemas=schemas, hook=self.hook, unpack=self.unpack)

    def unpack(self, signature, request, response, *args, **kwargs) -> dict:
        """
        Use the top-level properties of the request JSON payload object as the
        arguments to request handlers.
        """
        args_dict = {}
        kwargs_dict = kwargs.copy()
        print('kwargs_dict', kwargs_dict)
        print('args_dict', args_dict)

        for k, param in signature.parameters.items():
            if k in kwargs_dict:
                continue
            if param.default is inspect._empty:
                args_dict[k] = request.json[k]
            else:
                kwargs_dict[k] = request.json.get(k)

        args_dict.update(kwargs_dict)
        return args_dict

    def pack(self, data, request, response, *args, **kwargs):
        response.body = data

    def hook(self, handler: ApiHandler):
        """
        When a callable is registered with an ApiRegistry using an HTTP method
        decorator this "hook" method executes, which inspects the HTTP method,
        URL path, and other information in order to create a dynamic "resource"
        class with which to register said callable with Falcon as a route.
        """
        http_method = handler.decorator.http_method
        url_path = handler.decorator.path
        resource = self._resources.get(url_path)

        if resource is None:
            # build a so-called "resource" class dynamically
            ApiResource = self._resource_manager.new_resource_class(url_path)

            # register the pybiz ApiHandler (which in turn calls
            # the callable registered by the developer through one
            # of the registry's decorator methods.
            resource = ApiResource()
            resource.add_handler(http_method, handler)
            self._resources[url_path] = resource
            self._falcon_api.add_route(url_path, resource)

        # add an instance method to the resource singleton dynamically
        #resource.add_handler(http_method, handler)
