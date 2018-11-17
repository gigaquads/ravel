import inspect

from typing import List, Type, Dict

from appyratus.json import JsonEncoder
from appyratus.types import DictAccessor

from pybiz.manifest import Manifest

from .registry_decorator import RegistryDecorator
from .registry_middleware import RegistryMiddleware
from .registry_proxy import RegistryProxy


class Registry(object):
    def __init__(
        self,
        manifest: Manifest = None,
        middleware: List[RegistryMiddleware] = None
    ):
        self._decorators = []
        self._proxies = []
        self._manifest = manifest
        self._is_bootstrapped = False
        self._middleware = middleware or []
        self._json_encoder = JsonEncoder()

    def __call__(self, *args, **kwargs):
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
    def manifest(self):
        return self._manifest

    @property
    def middleware(self):
        return self._middleware

    @property
    def proxies(self):
        return self._proxies

    @property
    def decorators(self):
        return self._decorators

    @property
    def biz_types(self) -> DictAccessor:
        return self._manifest.biz_types

    @property
    def dao_types(self) -> DictAccessor:
        return self._manifest.dao_types

    @property
    def schemas(self) -> DictAccessor:
        return self._manifest.schemas

    @property
    def is_bootstrapped(self):
        return self._is_bootstrapped

    def bootstrap(self, manifest_filepath: str=None, defer_processing=False):
        """
        Bootstrap the data, business, and service layers, wiring them up,
        according to the settings contained in a service manifest file.

        Args:
            - filepath: Path to manifest.yml file
            - defer_processing: If set, the manifest file will only be loaded
              but process will not be called. This is useful if we need to
              perform additional logic beforehand.
        """
        if not self.is_bootstrapped:
            if (self._manifest is None) or (filepath is not None):
                self._manifest = Manifest(manifest_filepath)
            if (self.manifest is not None) and (not defer_processing):
                self._manifest.process()
            self._is_bootstrapped = True

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

    def start(self, *args, **kwargs):
        """
        Enter the main loop in whatever program context your Registry is
        being used, like in a web framework or a REPL.
        """
        raise NotImplementedError('override in subclass')

    def on_decorate(self, proxy: 'RegistryProxy'):
        """
        We come here whenever a function is decorated by this registry. Here we
        can add the decorated function to, say, a web framework as a route.
        """

    def on_request(self, proxy, signature, *args, **kwargs):
        """
        This executes immediately before calling a registered function. You
        must return re-packaged args and kwargs here. However, if nothing is
        returned, the raw args and kwargs are used.
        """
        return (args, kwargs)

    def on_response(self, proxy, result, *args, **kwargs):
        """
        The return value of registered callables come here as `result`. Here
        any global post-processing can be done. Args and kwargs consists of
        whatever raw data was passed into the callable *before* on_request
        executed.
        """
        return result
