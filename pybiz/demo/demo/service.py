import os

from pybiz.biz import BizObject
from pybiz.dao import Dao
from pybiz.api.falcon import FalconWsgiService
from pybiz.api.repl import Repl
from pybiz.api.falcon.middleware import JsonTranslator


def api_factory(api_type: str = None):
    """
    Returns one of the following:
        - 'web' -> Falcon web service
        - 'repl' -> IPython REPL session
    """
    api_type = api_type or os.environ.get('API_TYPE', 'web')
    if api_type == 'web':
        return DemoFalconService()
    elif api_type == 'repl':
        return DemoRepl(name='Demo', version='1.0.0')
    else:
        raise ValueError('unrecognized api type')


class DemoFalconService(FalconWsgiService):

    @property
    def middleware(self):
        return [JsonTranslator()]


class DemoRepl(Repl):
    pass
