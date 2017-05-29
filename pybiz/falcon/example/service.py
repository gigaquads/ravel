import json

from pybiz.falcon import Api


class UserService(Api):

    @property
    def middleware(self):
        return [
            JsonTranslator(),
            ]


class JsonTranslator(object):

    def process_request(self, request, response):
        pass

    def process_resource(self, request, response, resource, params):
        pass

    def process_response(self, request, response, resource):
        response.body = json.dumps(response.body)


api = UserService()
