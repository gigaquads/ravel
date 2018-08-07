import os

from pybiz.dao import Dao
from pybiz.api.falcon import FalconWsgiService
from pybiz.api.falcon.middleware import JsonTranslator
from pybiz.api.repl import Repl
from pybiz.biz import BizObject


class DemoFalconService(FalconWsgiService):

    @property
    def middleware(self):
        return [JsonTranslator()]


class DemoRepl(Repl):
    def __init__(self):
        super().__init__(name='demo', version='1.0.0')
