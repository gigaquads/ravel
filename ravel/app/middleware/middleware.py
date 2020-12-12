import inspect

from typing import Dict, Tuple, Set, Type, List

from ravel.util.misc_functions import get_class_name
from ravel.util.loggers import console


def virtual(func):
    """
    This decorator is used to mark methods as "virtual" on the base
    Middleware class. This flag is picked up at runtime, and these methods
    are skipped during ravel requrest processing (via Actions).
    """
    func.is_virtual = True
    return func


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
            f'bootstrapping {get_class_name(self)} middleware singleton'
        )

        self.on_bootstrap()
        self._is_bootstrapped = True

    @property
    def is_bootstrapped(self) -> bool:
        return self._is_bootstrapped

    @property
    def app(self) -> 'Application':
        """
        The Application using this middleware.
        """
        return self._app

    @property
    def app_types(self) -> Tuple[Type['Application']]:
        """
        Return a tuple of Application class objects for which this middleware
        applies.
        """
        import ravel

        return (ravel.Application, )

    def on_bootstrap(self):
        """
        Developer-defined custom logic triggered while bootstrapping.
        """

    @virtual
    def pre_request(
        self,
        action: 'Action',
        request: 'Request',
        raw_args: Tuple,
        raw_kwargs: Dict
    ):
        """
        In `pre_request`, `raw_args` and `raw_kwargs` contain the raw args and
        kwargs made available to us by the host Application. For example, in a a
        Ravel application backed by a web framework, the contents of `raw_args`
        could be the request and response objects.
        """

    @virtual
    def on_request(
        self,
        action: 'Action',
        request: 'Request',
        processed_args: Tuple,
        processed_kwargs: Dict
    ):
        """
        At the point `on_request` runs, the host application has transformed the
        `raw_args` and `raw_kwargs` into their processed counterparts, which
        represent the args and kwargs expected by the underyling action
        callable.
        """

    @virtual
    def post_request(
        self,
        action: 'Action',
        request: 'Request',
        result,
    ):
        """
        If no exception was raised in the action callable, we come here.
        """

    @virtual
    def post_bad_request(
        self,
        action: 'Action',
        request: 'Request',
        exc: Exception,
    ):
        """
        If any exception was raised in the action callable, we come here.
        """
