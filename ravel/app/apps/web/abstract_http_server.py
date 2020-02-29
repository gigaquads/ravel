import traceback

from collections import defaultdict
from typing import Text, Type, Callable

import requests

from appyratus.utils import StringUtils

from ravel.app.base import Application, Action, ActionDecorator
from ravel.util import get_class_name
from ravel.util.misc_functions import get_callable_name


class AbstractHttpServer(Application):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.route_2_endpoint = defaultdict(dict)

    @property
    def decorator_type(self) -> Type['EndpointDecorator']:
        return EndpointDecorator

    @property
    def action_type(self) -> Type['Endpoint']:
        return Endpoint

    def route(self, method, route, args=None, kwargs=None):
        method = method.lower()
        route = route.lower()
        method2endpoint = self.route_2_endpoint.get(route)
        if method2endpoint:
            endpoint = method2endpoint.get(method)
            return endpoint(*(args or tuple()), **(kwargs or dict()))
        return None

    def client(self, host, port, scheme='http'):
        return HttpClient(self, scheme, host, port)


class EndpointDecorator(ActionDecorator):
    def __init__(self,
        app: 'AbstractHttpServer',
        method: Text,
        route: Text = None,
        *args,
        **kwargs
    ):
        super().__init__(app, *args, **kwargs)
        self.method = method.lower()
        self.route = route.lower() if route else None

    def __call__(self, target: Callable) -> 'Endpoint':
        """
        We wrap each registered func in a Action and store it in a table
        that lets us look it up by route and method for use in routing
        requests.
        """
        # default null route to the name of the target callable
        if self.route is None:
            if isinstance(target, Action):
                self.route = StringUtils.dash(target.name.lower())
            else:
                self.route = StringUtils.dash(get_callable_name(target).lower())

        if not self.route.startswith('/'):
            self.route = '/' + self.route

        endpoint = super().__call__(target)
        self.app.route_2_endpoint[self.route][self.method] = endpoint
        return endpoint


class Endpoint(Action):
    """
    Stores metadata related to the "target" callable, which in the Http context
    is the action of some URL route.
    """

    def __init__(self, target, decorator):
        super().__init__(target, decorator)
        self.method = decorator.method
        self.route = decorator.route

    def __repr__(self):
        return '{}({})'.format(
            get_class_name(self),
            ', '.join([
                f'name={self.name}',
                f'method={self.decorator.method.upper()}',
                f'route={self.decorator.route}',
            ])
        )


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

        for method2endpoint in self._app.route_2_endpoint.values():
            for method, endpoint in method2endpoint .items():
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
