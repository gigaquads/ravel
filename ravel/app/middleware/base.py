import inspect

from typing import Dict, Tuple, Set, Type, List

from appyratus.memoize import memoized_property

from ravel.util.misc_functions import get_class_name
from ravel.util.loggers import console


class MiddlewareError(Exception):
    def __init__(self, middleware, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.middleware = middleware


class Middleware(object):
    def __init__(self, *args, **kwargs):
        self._is_bootstrapped = False

    def __repr__(self):
        return (
            f'Middleware(name={get_class_name(self)})'
        )

    def bootstrap(self, app: 'Application'):
        self._app = app

        console.debug(
            f'bootstrapping "{get_class_name(self)}" middleware'
        )

        self.on_bootstrap()
        self._is_bootstrapped = True

    def on_bootstrap(self):
        pass

    @property
    def is_bootstrapped(self) -> bool:
        return self._is_bootstrapped

    @property
    def app(self) -> 'Application':
        return self._app

    @memoized_property
    def app_types(self) -> Tuple[Type['Application']]:
        """
        Return a tuple of Application class objects for which this middleware
        applies.
        """
        import ravel

        return (ravel.Application, )

    def pre_request(
        self,
        endpoint: 'Endpoint',
        raw_args: Tuple,
        raw_kwargs: Dict
    ):
        """
        In `pre_request`, `raw_args` and `raw_kwargs` contain the raw args and
        kwargs made available to us by the host Application. For example, in a a
        Pybiz application backed by a web framework, the contents of `raw_args`
        could be the request and response objects.
        """

    def on_request(
        self,
        endpoint: 'Endpoint',
        raw_args: Tuple,
        raw_kwargs: Dict,
        processed_args: Tuple,
        processed_kwargs: Dict
    ):
        """
        At the point `on_request` runs, the host application has transformed the
        `raw_args` and `raw_kwargs` into their processed counterparts, which
        represent the args and kwargs expected by the underyling endpoint
        callable.
        """

    def post_request(
        self,
        endpoint: 'Endpoint',
        raw_args: Tuple,
        raw_kwargs: Dict,
        processed_args: Tuple,
        processed_kwargs: Dict,
        result,
    ):
        """
        If no exception was raised in the endpoint callable, we come here.
        """

    def post_bad_request(
        self,
        endpoint: 'Endpoint',
        raw_args: Tuple,
        raw_kwargs: Dict,
        processed_args: Tuple,
        processed_kwargs: Dict,
        exc: Exception,
    ):
        """
        If any exception was raised in the endpoint callable, we come here.
        """
