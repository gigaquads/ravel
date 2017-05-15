from pybiz.falcon import Api
from pybiz.falcon.example.middleware import JsonTranslator


class UserService(Api):

    @property
    def middleware(self):
        return [
            JsonTranslator(),
            ]


api = UserService()
