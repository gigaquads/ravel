import traceback

from collections import defaultdict
from typing import Text, Type, Callable, List

import requests

from appyratus.utils import StringUtils

from ravel.app.base import Application, Action, ActionDecorator
from ravel.util import get_class_name
from ravel.util.misc_functions import get_callable_name


DEFAULT_HOST = 'localhost'
DEFAULT_PORT = 8081

class AbstractHttpServer(Application):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.route_2_endpoint = defaultdict(dict)
        self.host = None
        self.port = None

    @property
    def decorator_type(self) -> Type['EndpointDecorator']:
        return EndpointDecorator

    @property
    def action_type(self) -> Type['Endpoint']:
        return Endpoint

    def on_decorate(self, endpoint: 'Endpoint'):
        endpoint.routes.append(
            f'/{StringUtils.dash(endpoint.name).lower()}'
        )
        for route in endpoint.routes:
            if route in self.route_2_endpoint:
                self.app.route_2_endpoint[route][endpoint.method] = endpoint

    def on_bootstrap(self, host: Text = None, port: int = None):
        self.host = host or DEFAULT_HOST
        self.port = port or DEFAULT_PORT

    def route(self, method, route, args=None, kwargs=None):
        method = method.lower()
        route = route.lower()
        method_2_endpoint = self.route_2_endpoint.get(route)
        if method_2_endpoint:
            endpoint = method_2_endpoint.get(method)
            return endpoint(*(args or tuple()), **(kwargs or dict()))
        return None

    def client(self, host, port, scheme='http'):
        return HttpClient(self, scheme, host, port)



class EndpointDecorator(ActionDecorator):
    def __init__(self,
        app: 'AbstractHttpServer',
        method: Text,
        route: Text = None,
        routes: List[Text] = None,
        *args, **kwargs
    ):
        super().__init__(app, *args, **kwargs)
        self.method = method.lower()
        self.routes = self._build_routes_list(route, routes)

    def _build_routes_list(self, route, routes):
        """
        Combine `route` and `routes` constructor kwargs.
        """
        route_set = set()
        if route is not None:
            if isinstance(route, (list, tuple, set)):
                route_set.update(route)
            else:
                assert isinstance(route, str)
                route_set.add(route)
        if routes:
            route_set.update(routes)
        # normalize all routes to lower case and ensure they all begin with a
        # single forward slash and no trailing slash.
        return [
            '/' + route.strip('/').lower()
            for route in route_set
        ]


class Endpoint(Action):
    """
    Stores metadata related to the "target" callable, which in the Http context
    is the action of some URL route.
    """

    def __init__(self, target, decorator):
        super().__init__(target, decorator)
        self.method = decorator.method
        self.routes = decorator.routes

    def __repr__(self):
        return '{}({})'.format(
            get_class_name(self),
            ', '.join([
                f'name={self.name}',
                f'method={self.decorator.method.upper()}',
                f'routes={self.decorator.routes}',
            ])
        )

    @classmethod
    def from_function(cls, app, func, method: str, route: str) -> 'Endpoint':
        return cls(func, EndpointDecorator(app, method=method, route=route))


class HttpClient(object):
    def __init__(
        self,
        app: 'AbstractHttpServer',
        scheme: Text,
        host: Text,
        port: int,
    ):
        self._app = app
        self._handlers = {}
        self._host = host
        self._port = port
        self._scheme = scheme

        for method_2_endpoint in self._app.route_2_endpoint.values():
            for method, endpoint in method_2_endpoint .items():
                self._handlers[endpoint.name] = self._build_handler(
                    method, endpoint
                )

    def __getattr__(self, route: Text) -> Callable:
        handler = self._handlers[route]
        return handler

    def _build_handler(self, method: Text, endpoint: 'Endpoint') -> Callable:
        def handler(data=None, json=None, params=None, headers=None, path=None):
            route = (
                endpoint.route if not path
                else endpoint.route.format(**path)
            )
            url = ('{}://{}:{}/' + route.strip('/')).format(
                self._scheme, self._host, self._port
            )
            return requests.request(
                method=method,
                url=url,
                json=json,
                params=params,
                headers=headers,
            )
        return handler
