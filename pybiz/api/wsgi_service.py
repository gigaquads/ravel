from pybiz.api.http import HttpFunctionRegistry


class WsgiService(HttpFunctionRegistry):
    def start(self, environ=None, start_response=None, *args, **kwargs):
        raise NotImplementedError('override in subclass')
