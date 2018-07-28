import ujson

from pybiz.util import is_bizobj


class JsonTranslator(object):
    def __init__(self, encode=None):
        self.encode = encode or ujson.dumps

    def process_request(self, request, response):
        if request.content_length:
            request.json = ujson.loads(request.stream.read().decode())
        else:
            request.json = {}

    def process_resource(self, request, response, resource, params):
        pass

    def process_response(self, request, response, resource):
        response.body = self.encode(response.unserialized_body or {})
