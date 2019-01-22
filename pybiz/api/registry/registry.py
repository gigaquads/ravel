import inspect

from typing import List, Type, Dict, Tuple

from appyratus.utils import DictAccessor
from appyratus.memoize import memoized_property

from pybiz.manifest import Manifest
from pybiz.util import JsonEncoder

from .registry_decorator import RegistryDecorator
from .registry_proxy import RegistryProxy


class Registry(object):
    def __init__(
        self,
        manifest: Manifest = None,
        middleware: List['RegistryMiddleware'] = None
    ):
        self._decorators = []
        self._proxies = []
        self._manifest = manifest or Manifest()
        self._is_bootstrapped = False
        self._middleware = middleware or []
        self._json_encoder = JsonEncoder()

    def __call__(self, *args, **kwargs) -> RegistryDecorator:
        """
        Use this to decorate functions, adding them to this Registry.
        Each time a function is decorated, it arives at the "on_decorate"
        method, where you can registry the function with a web framework or
        whatever system you have in mind.

        Usage:

        ```python3
            api = Registry()

            @api()
            def do_something():
                pass
        ```
        """
        # build and the decorator
        decorator = self.decorator_type(self, *args, **kwargs)
        self.decorators.append(decorator)
        return decorator

    @property
    def decorator_type(self) -> Type[RegistryDecorator]:
        return RegistryDecorator

    @property
    def proxy_type(self) -> Type[RegistryProxy]:
        return RegistryProxy

    @property
    def manifest(self) -> Manifest:
        return self._manifest

    @memoized_property
    def middleware(self) -> List['RegistryMiddleware']:
        return [
            m for m in self._middleware
            if isinstance(self, m.registry_types)
        ]

    @property
    def proxies(self) -> List[RegistryProxy]:
        return self._proxies

    @property
    def decorators(self) -> List[RegistryDecorator]:
        return self._decorators

    @property
    def types(self) -> DictAccessor:
        return self._manifest.types

    @property
    def is_bootstrapped(self):
        return self._is_bootstrapped

    def bootstrap(self):
        """
        Bootstrap the data, business, and service layers, wiring them up.
        Override in subclass.
        """
        self._is_bootstrapped = True

    def start(self, *args, **kwargs):
        """
        Enter the main loop in whatever program context your Registry is
        being used, like in a web framework or a REPL.
        """
        raise NotImplementedError('override in subclass')

    def dump(self) -> Dict:
        """
        Return a Python dict that can be serialized to JSON, represents the
        contents of the Registry. The purpose of this method is to export
        metadata about this registry to be consumed by some other service or
        external process without said service or process needing to import this
        Registry directly.
        """
        return {
            'registry': {p.dump() for p in self.proxies}
        }

    def on_decorate(self, proxy: 'RegistryProxy'):
        """
        We come here whenever a function is decorated by this registry. Here we
        can add the decorated function to, say, a web framework as a route.
        """

    def on_request(self, proxy, *args, **kwargs) -> Tuple[Tuple, Dict]:
        """
        This executes immediately before calling a registered function. You
        must return re-packaged args and kwargs here. However, if nothing is
        returned, the raw args and kwargs are used.
        """
        return (args, kwargs)

    def on_response(self, proxy, result, *args, **kwargs) -> object:
        """
        The return value of registered callables come here as `result`. Here
        any global post-processing can be done. Args and kwargs consists of
        whatever raw data was passed into the callable *before* on_request
        executed.
        """
        return result
