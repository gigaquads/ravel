import ujson

from pybiz.util import is_bizobj


class Middleware(object):
    def __init__(self, *args, **kwargs):
        pass

    def process_request(self, request, response):
        pass

    def process_resource(self, request, response, resource, params):
        pass

    def process_response(self, request, response, resource):
        pass

    def bind(self, service):
        pass
