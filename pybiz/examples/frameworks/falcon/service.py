import json

from pybiz.frameworks.falcon import Api
from pybiz.frameworks.falcon.middleware import JsonTranslator


class UserService(Api):

    @property
    def middleware(self):
        return [
            JsonTranslator(),
            ]



api = UserService.get_instance()
