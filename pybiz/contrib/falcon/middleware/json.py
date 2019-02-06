import ujson

from pybiz.util import is_bizobj
from pybiz.json import JsonEncoder

from .base import Middleware


class JsonBodyMiddleware(Middleware):
    def __init__(self, encode=None):
        self.encode = encode or ujson.dumps

    def pre_request(self, args, kwargs):
        pass

    def on_request(self, args, kwargs, prepared_args, prepared_kwargs):
        pass

    def post_request(
        self, args, kwargs, prepared_args, prepared_kwargs, result
    ):
        pass

    def process_request(self, request, response):
        if request.content_length:
            request.json = JsonEncoder.decode(request.stream.read().decode())
        else:
            request.json = {}

    def process_resource(self, request, response, resource, params):
        pass

    def process_response(self, request, response, resource):
        unserialized_body = getattr(response, 'unserialized_body', None)
        if unserialized_body:
            if is_bizobj(unserialized_body):
                unserialized_body = unserialized_body.dump()
        response.body = self.encode(unserialized_body or {})
