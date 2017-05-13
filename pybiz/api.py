from collections import defaultdict

from .const import (
    HTTP_GET,
    HTTP_POST,
    HTTP_PUT,
    HTTP_PATCH,
    HTTP_DELETE,
    )


class registry(object):

    methods = defaultdict(dict)

    def __init__(self, http_method: str, path: str, schemas: dict = None):
        self.http_method = http_method.lower()
        self.path = path.lower()
        self.schemas = schemas or {}

    def __call__(self, func):
        schemas = self.schemas

        def handler(*args, **kwargs):
            if 'request' in schemas:
                request = args[0]
                request_data = self.load_request_json(request)
                schema = schemas.get('request')
                result = schema.load(request_data, strict=True)
                request.json = result.data

            handler_result = func(*args, **kwargs)
            print(handler_result)

            if 'response' in schemas:
                schema = schemas.get('response')
                handler_result = schema.load(handler_result, strict=True)

            return handler_result

        self.methods[self.path][self.http_method] = handler
        handler.__name__ = 'on_{}'.format(self.http_method)
        return handler

    @classmethod
    def route(cls, http_method, path, handler_args=None, handler_kwargs=None):
        handler = cls.methods[path.lower()][http_method.lower()]
        handler_args = handler_args or tuple()
        handler_kwargs = handler_kwargs or dict()
        return handler(*handler_args, **handler_kwargs)


class get(registry):
    def __init__(self, path, schemas = None):
        super(get, self).__init__(HTTP_GET, path, schemas)


class post(registry):
    def __init__(self, path, schemas = None):
        super(post, self).__init__(HTTP_POST, path, schemas)


class patch(registry):
    def __init__(self, path, schemas = None):
        super(patch, self).__init__(HTTP_PATCH, path, schemas)


class put(registry):
    def __init__(self, path, schemas = None):
        super(put, self).__init__(HTTP_PUT, path, schemas)


class delete(registry):
    def __init__(self, path, schemas = None):
        super(delete, self).__init__(HTTP_DELETE, path, schemas)
