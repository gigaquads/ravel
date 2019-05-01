from .http_registry import HttpRegistry


class WsgiServiceRegistry(HttpRegistry):
    def on_start(self):
        def wsgi_entrypoint(environ=None, start_response=None, *args, **kwargs):
            raise NotImplementedError('override in subclass')

        return wsgi_entrypoint
