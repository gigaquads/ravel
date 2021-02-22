import traceback

from collections import defaultdict
from typing import Text, Type, Callable, List, Dict

import requests

from appyratus.utils.string_utils import StringUtils

from ravel.app.base import Application, Action, ActionDecorator
from ravel.util import get_class_name
from ravel.util.misc_functions import get_callable_name
from ravel.util.type_checking import is_resource, is_batch
from ravel.util.loggers import console

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
            if route not in self.route_2_endpoint:
                console.debug(f'routing {route} to {endpoint.name} action')
                self.route_2_endpoint[route][endpoint.method] = endpoint

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

    def build_client(self, host, port, scheme='http'):
        if not self.is_bootstrapped:
            raise Exception(f'{self} must be bootstrapped')
        return HttpClient(self, scheme, host, port)


class EndpointDecorator(ActionDecorator):
    def __init__(self,
        app: 'AbstractHttpServer',
        method: Text,
        route: Text = None,
        routes: List[Text] = None,
        path: Text = None,
        paths: List[Text] = None,
        *args, **kwargs
    ):
        super().__init__(app, *args, **kwargs)
        self.method = method.lower()

        routes = set(self._build_routes_list(route, routes))
        routes |= set(self._build_routes_list(path, paths))
        self.routes = list(routes)

    @staticmethod
    def _build_routes_list(route, routes):
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
        headers: Dict = None,
    ):
        self._app = app
        self._handlers = {}
        self._host = host
        self._port = port
        self._scheme = scheme
        self._endpoint_name_2_route = {}
        self._response_hooks = []
        self._request_hooks = []
        self.headers = headers or {}

        for method_2_endpoint in self._app.route_2_endpoint.values():
            for http_method, endpoint in method_2_endpoint.items():
                primary_route = endpoint.routes[0]
                self._handlers[endpoint.name] = self._build_handler(
                    endpoint.name, http_method, primary_route
                )

    def __getattr__(self, route: Text) -> Callable:
        handler = self._handlers[route]
        return handler

    def _build_handler(self, name, method: Text, route: Text) -> Callable:
        self._endpoint_name_2_route[name] = route

        def handler(json=None, data=None, params=None, headers=None, path=None):

            def replace_objects_with_ids(data: Dict):
                data = data.copy()
                for k, v in list(data.items()):
                    if is_resource(v) or is_batch(v):
                        data[k] = v._id
                return data

            if isinstance(json, dict):
                json = replace_objects_with_ids(json)

            path_vars = replace_objects_with_ids(
                dict(
                    json or {},
                    **(data if isinstance(data, dict) else {}),
                    **(path or {})
                )
            )

            route = self._endpoint_name_2_route[name].format(**path_vars)
            headers = dict(self.headers, **(headers or {}))
            url = ('{}://{}:{}/' + route.strip('/')).format(
                self._scheme, self._host, self._port
            )

            kwargs = {
                'method': method,
                'url': url,
                'json': json,
                'params': params,
                'headers': headers,
            }

            for hook in self._request_hooks:
                hook(self, **kwargs)

            response = requests.request(**kwargs)

            for hook in self._response_hooks:
                hook(self, response, **kwargs)

            return response

        return handler

    def add_request_hook(self, hook):
        self._request_hooks.append(hook)

    def add_response_hook(self, hook):
        self._response_hooks.append(hook)