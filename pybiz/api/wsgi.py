from pybiz.api.http import HttpRegistry


class WsgiServiceRegistry(HttpRegistry):
    def start(self, environ=None, start_response=None, *args, **kwargs):
        raise NotImplementedError('override in subclass')
