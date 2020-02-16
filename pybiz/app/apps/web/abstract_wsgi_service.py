from .abstract_http_server import AbstractHttpServer


class AbstractWsgiService(AbstractHttpServer):
    def on_start(self):
        def wsgi_entrypoint(environ=None, start_response=None, *args, **kwargs):
            raise NotImplementedError('override in subclass')

        return wsgi_entrypoint
