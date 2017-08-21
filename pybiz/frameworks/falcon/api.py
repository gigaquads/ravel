from __future__ import absolute_import

import importlib

import venusian

from pybiz.api import ApiRegistry
from pybiz.const import HTTP_GET, HTTP_POST, HTTP_PUT, HTTP_PATCH, HTTP_DELETE

from .resource import FalconResourceManager



class Api(ApiRegistry):

    def __init__(self, *args, **kwargs):
        try:
            import falcon
        except ImportError as exc:
            # this try/except is used here to avoid having to bake in
            # falcon as a global dependency of pybiz.
            exc.message = 'falcon must be installed'
            raise

        super(Api, self).__init__(*args, **kwargs)

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
        return None

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

    def unpack(self, request):
        """
        Unpack the request data into args and kwargs, returning them in a tuple:
        (args, kwargs).
        """
        return None

    def hook(self, handler):
        """
        This decorator "hook" registers a wrapped method with Falcon.
        """
        http_method = handler.decorator.http_method
        url_path = handler.decorator.path
        if url_path not in self._resources:
            ApiResource = self._resource_manager.new_resource_class(url_path)
            resource = ApiResource()
            resource.add_handler(http_method, handler)
            self._resources[url_path] = resource
            self._falcon_api.add_route(url_path, resource)

    def validate_request(self, request, schema):
        request.json = schema.load(request.data, strict=True)

    def validate_params(self, request, schema):
        request.params = params_schema.load(request.params, strict=True)

    def validate_response(self, response, result, schema):
        response.body = schema.load(result, strict=True)
