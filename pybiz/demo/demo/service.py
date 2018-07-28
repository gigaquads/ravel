import os

from pybiz.biz import BizObject
from pybiz.dao import Dao
from pybiz.api.falcon import FalconWsgiService
from pybiz.api.falcon.middleware import JsonTranslator, RequestBinder


class DemoService(object):

    @staticmethod
    def factory():
        if os.environ.get('DEMO_API', 'web') == 'web':
            return DemoFalconService()
        else:
            raise ValueError('unrecognized api type')


class DemoFalconService(FalconWsgiService):

    @property
    def middleware(self):
        return [
            RequestBinder(objects=[Dao, BizObject]),
            JsonTranslator(),
        ]
