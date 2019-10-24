import inspect

from typing import Dict, Tuple, Set, Type, List

from appyratus.memoize import memoized_property


class ApplicationMiddleware(object):
    def __init__(self, *args, **kwargs):
        self._is_bootstrapped = False

    def __repr__(self):
        return (
            f'<Middleware({self.__class__.__name__})>'
        )

    def bootstrap(self, app: 'Application'):
        self._app = app
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
        import pybiz

        return (pybiz.Application, )

    def pre_request(self, endpoint: 'Endpoint', raw_args: List, raw_kwargs: Dict):
        """
        In pre_request, args and kwargs are in the raw form before being
        processed by app.on_request.
        """

    def on_request(self, endpoint: 'Endpoint', args: List, kwargs: Dict):
        """
        In on_request, args and kwargs are in the form output by
        api.on_request.
        """

    def post_request(
        self, endpoint: 'ApplicationObject', raw_args: List, raw_kwargs: Dict,
        args: List, kwargs: Dict, result, exc: Exception = None
    ):
        """
        In post_request, args and kwargs are in the form output by
        app.on_request.
        """
