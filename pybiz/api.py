from collections import defaultdict

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
