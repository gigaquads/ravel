import traceback
import requests

from collections import defaultdict

from ravel.app.base import Application, Action, ActionDecorator


class AbstractHttpServer(Application):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.routes = defaultdict(dict)

    @property
    def decorator_type(self):
        return HttpDecorator

    @property
    def action_type(self):
        return HttpRoute

    def route(self, http_method, url_path, args=None, kwargs=None):
        http_method = http_method.lower()
        url_path = url_path.lower()
        http_method2action = self.routes.get(url_path)
        if http_method2action:
            route = http_method2action.get(http_method)
            return route(*(args or tuple()), **(kwargs or dict()))
        return None

    def client(self, host, port, scheme='http'):
        return HttpClient(self, scheme, host, port)


class HttpDecorator(ActionDecorator):
    def __init__(self,
        app,
        http_method: str,
        url_path: str,
        schemas: dict=None,
        authorize=None,
        on_decorate=None,
        on_request=None,
        on_response=None,
        *args,
        **kwargs
    ):
        super().__init__(
            app,
            on_decorate=on_decorate,
            on_request=on_request,
            on_response=on_response,
            *args, **kwargs,
        )
        self.http_method = http_method.lower()
        self.url_path = url_path.lower()
        self.schemas = schemas
        self.authorize = authorize

    def __call__(self, func):
        """
        We wrap each registered func in a Action and store it in a table
        that lets us look it up by url_path and http_method for use in routing
        requests.
        """
        route = super().__call__(func)
        self.app.routes[self.url_path][self.http_method] = route
        return route


class HttpRoute(Action):
    """
    Stores metadata related to the "target" callable, which in the Http context
    is the action of some URL route.
    """

    def __init__(self, func, decorator):
        super().__init__(func, decorator)
        self.http_method = decorator.http_method
        self.url_path = decorator.url_path
        self.schemas = decorator.schemas
        self.authorize = decorator.authorize

    def __repr__(self):
        return '<{}({})>'.format(
            self.__class__.__name__,
            ', '.join([
                f'name="{self.name}"',
                f'method={self.decorator.http_method.upper()}',
                f'path={self.decorator.url_path}',
            ])
        )


class HttpClient(object):
    def __init__(self, app: AbstractHttpServer, scheme, host, port):
        self._app = app
        self._handlers = {}
        self._host = host
        self._port = port
        self._scheme = scheme
        for http_method2route in self._app.routes.values():
            for http_method, route in http_method2route.items():
                self._handlers[route.name] = self._build_handler(
                    http_method, route
                )

    def _build_handler(self, http_method, route):
        def handler(data=None, json=None, params=None, headers=None, path=None):
            url_path = (
                route.url_path if not path
                else route.url_path.format(**path)
            )
            url = ('{}://{}:{}/' + url_path.strip('/')).format(
                self._scheme, self._host, self._port
            )
            return requests.request(
                method=http_method,
                url=url,
                json=json,
                params=params,
                headers=headers,
            )
        return handler

    def __getattr__(self, route_name):
        handler = self._handlers[route_name]
        return handler
