from collections import defaultdict

from .const import (
    HTTP_GET,
    HTTP_POST,
    HTTP_PUT,
    HTTP_PATCH,
    HTTP_DELETE,
    )


class ApiRegistry(object):

    def __init__(self):
        self.handlers = defaultdict(dict)

    def get(self, path, schemas=None, hook=None):
        return ApiRegistryDecorator(self, HTTP_GET, path,
                schemas=schemas, hook=hook)

    def post(self, path, schemas=None):
        return ApiRegistryDecorator(self, HTTP_POST, path, schemas=schemas)

    def put(self, path, schemas=None):
        return ApiRegistryDecorator(self, HTTP_PUT, path, schemas=schemas)

    def patch(self, path, schemas=None):
        return ApiRegistryDecorator(self, HTTP_PATCH, path, schemas=schemas)

    def delete(self, path, schemas=None):
        return ApiRegistryDecorator(self, HTTP_DELETE, path, schemas=schemas)

    def route(self, http_method, path, handler_args=None, handler_kwargs=None):
        handler = self.handlers[path.lower()][http_method.lower()]
        handler_args = handler_args or tuple()
        handler_kwargs = handler_kwargs or dict()
        return handler(*handler_args, **handler_kwargs)

    def validate_request(self, schema, params_schema, *args, **kwargs):
        pass

    def validate_response(self, schema, result, *args, **kwargs):
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
        # TODO: Move this schema logic into pre middleware hooks passed to ctor
        # as a `validate={request: func, response: func, params: func}` kwarg in
        # ApiRegistry
        schemas = self.decorator.schemas
        if 'request' in schemas:
            request = args[0]
            request_data = request.json  # TODO: implement this in falcon with Request subclass
            schema = schemas.get('request')
            result = schema.load(request_data, strict=True)
            request.json = result.data

        request_schema = schemas.get('request')
        response_schema = schemas.get('response')
        params_schema = schemas.get('params')

        self.decorator.registry.validate_request(
                request_schema, params_schema, *args, **kwargs)

        handler_result = self.target(*args, **kwargs)

        self.decorator.registry.validate_response(
                response_schema, handler_result, *args, **kwargs)

        if 'response' in schemas:
            schema = schemas.get('response')
            handler_result = schema.load(handler_result, strict=True)

        return handler_result
