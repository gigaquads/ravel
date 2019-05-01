import inspect

from typing import Dict, Tuple, Set, Type

from appyratus.memoize import memoized_property


class RegistryMiddleware(object):
    def __init__(self, *args, **kwargs):
        self._is_bootstrapped = False
        
    def __repr__(self):
        return (
            f'<Middleware({self.__class__.__name__}, '
            f'bootstrapped={self._is_bootstrapped})>'
        )

    def bootstrap(self, registry: 'Registry'):
        self._registry = registry
        self.on_bootstrap()
        self._is_bootstrapped = True

    def on_bootstrap(self):
        pass

    @property
    def is_bootstrapped(self) -> bool:
        return self._is_bootstrapped

    @property
    def registry(self) -> 'Registry':
        return self._registry

    @memoized_property
    def registry_types(self) -> Tuple[Type['Registry']]:
        """
        Return a tuple of Registry class objects for which this middleware
        applies.
        """
        from pybiz.api.registry import Registry

        return (Registry, )

    def pre_request(
        self,
        proxy: 'RegistryProxy',
        args: Tuple,
        kwargs: Dict
    ):
        """
        In pre_request, args and kwargs are in the raw form before being
        processed by registry.on_request.
        """

    def on_request(
        self,
        proxy: 'RegistryProxy',
        args: Tuple,
        kwargs: Dict
    ):
        """
        In on_request, args and kwargs are in the form output by
        registry.on_request.
        """

    def post_request(
        self,
        proxy: 'RegistryObject',
        raw_args: Tuple,
        raw_kwargs: Dict,
        args: Tuple,
        kwargs: Dict,
        result,
        exc: Exception = None
    ):
        """
        In post_request, args and kwargs are in the form output by
        registry.on_request.
        """
