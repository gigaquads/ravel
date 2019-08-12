import inspect

from typing import Dict, Tuple, Set, Type, List

from appyratus.memoize import memoized_property


class ApiMiddleware(object):
    def __init__(self, *args, **kwargs):
        self._is_bootstrapped = False

    def __repr__(self):
        return (
            f'<Middleware({self.__class__.__name__}, '
            f'bootstrapped={self._is_bootstrapped})>'
        )

    def bootstrap(self, api: 'Api'):
        self._api = api
        self.on_bootstrap()
        self._is_bootstrapped = True

    def on_bootstrap(self):
        pass

    @property
    def is_bootstrapped(self) -> bool:
        return self._is_bootstrapped

    @property
    def api(self) -> 'Api':
        return self._api

    @memoized_property
    def api_types(self) -> Tuple[Type['Api']]:
        """
        Return a tuple of Api class objects for which this middleware
        applies.
        """
        from pybiz.api.base import Api

        return (Api, )

    def pre_request(self, proxy: 'ApiProxy', raw_args: List, raw_kwargs: Dict):
        """
        In pre_request, args and kwargs are in the raw form before being
        processed by api.on_request.
        """

    def on_request(self, proxy: 'ApiProxy', args: List, kwargs: Dict):
        """
        In on_request, args and kwargs are in the form output by
        api.on_request.
        """

    def post_request(
        self, proxy: 'ApiObject', raw_args: List, raw_kwargs: Dict,
        args: List, kwargs: Dict, result, exc: Exception = None
    ):
        """
        In post_request, args and kwargs are in the form output by
        api.on_request.
        """
