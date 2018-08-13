import traceback

from collections import defaultdict

from pybiz.exc import ApiError

from .base import FunctionRegistry, FunctionProxy, FunctionDecorator


class HttpFunctionRegistry(FunctionRegistry):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.routes = defaultdict(dict)

    @property
    def function_decorator_type(self):
        return HttpFunctionDecorator

    @property
    def function_proxy_type(self):
        return HttpRoute

    def on_decorate(self, route: 'HttpRoute'):
        pass

    def route(self, http_method, url_path, args=None, kwargs=None):
        http_method = http_method.lower()
        url_path = url_path.lower()
        http_method2proxy = self.routes.get(url_path)
        if http_method2proxy:
            route = http_method2proxy.get(http_method)
            return route(*(args or tuple()), **(kwargs or dict()))
        return None


class HttpFunctionDecorator(FunctionDecorator):
    def __init__(self,
        registry,
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
            registry,
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
        We wrap each registered func in a FunctionProxy and store it in a table
        that lets us look it up by url_path and http_method for use in routing
        requests.
        """
        route = super().__call__(func)
        self.registry.routes[self.url_path][self.http_method] = route
        return route


class HttpRoute(FunctionProxy):
    """
    Stores metadata related to the "target" callable, which in the Http context
    is the endpoint of some URL route.
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
                'method={}'.format(self.decorator.http_method.upper()),
                'path={}'.format(self.decorator.url_path),
            ])
        )
