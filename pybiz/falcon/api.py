from __future__ import absolute_import

from pybiz.api import ApiRegistry
from pybiz.const import HTTP_GET, HTTP_POST, HTTP_PUT, HTTP_PATCH, HTTP_DELETE
from pybiz.falcon.resource import FalconResourceManager



class Api(ApiRegistry):

    def __init__(self, middleware: list = None, *args, **kwargs):
        try:
            import falcon
        except ImportError as exc:
            # this try/except is used here to avoid having to bake in
            # falcon as a global dependency of pybiz.
            exc.message = 'falcon must be installed to use BizObjectApi'
            raise

        super(Api, self).__init__(*args, **kwargs)

        middleware = (middleware or []) + self.middleware

        self._falcon_api = falcon.API(middleware=middleware)
        self._resource_manager = FalconResourceManager()
        self._resources = {}

    @property
    def middleware(self):
        return []

    def __call__(self, environ, start_response):
        return self._falcon_api(environ, start_response)

    def get(self, path, schemas=None):
        return super(Api, self).get(path, schemas=schemas, hook=self.hook)

    def post(self, path, schemas=None):
        return super(Api, self).post(path, schemas=schemas, hook=self.hook)

    def patch(self, path, schemas=None):
        return super(Api, self).patch(path, schemas=schemas, hook=self.hook)

    def put(self, path, schemas=None):
        return super(Api, self).put(path, schemas=schemas, hook=self.hook)

    def delete(self, path, schemas=None):
        return super(Api, self).delete(path, schemas=schemas, hook=self.hook)

    def hook(self, handler):
        http_method = handler.decorator.http_method
        url_path = handler.decorator.path   # TODO: rename path to url_path
        if url_path not in self._resources:
            ApiResource = self._resource_manager.new_resource_class(url_path)
            resource = ApiResource()
            resource.add_handler(http_method, handler)
            self._resources[url_path] = resource
            self._falcon_api.add_route(url_path, resource)

        #resource = self._resources[url_path]
        #resource.add_handler(http_method, handler)

    def validate_request(self, schema, params_schema, request, *args, **kwargs):
        if schema is not None:
            request.json = schema.load(request.data, strict=True)
        if params_schema is not None:
            request.params = params_schema.load(request.params, strict=True)

    def validate_response(self, schema, result, request, response, *args, **kwargs):
        if schema is not None:
            response.body = schema.load(result, strict=True)
        else:
            response.body = result
