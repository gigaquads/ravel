import pybiz.frameworks.falcon

from pybiz import Dao, BizObject
from pybiz.frameworks.falcon.middleware import (
    RequestBinder,
    JsonTranslator,
)


class App(pybiz.frameworks.falcon.Api):

    @property
    def middleware(self):
        return [
            RequestBinder([Dao, BizObject]),
            JsonTranslator(encoder=None),
        ]
