import inspect

from typing import List, Type, Dict, Tuple, Text
from collections import deque

from appyratus.utils import DictAccessor, DictUtils
from appyratus.memoize import memoized_property

from pybiz.manifest import Manifest
from pybiz.json import JsonEncoder
from pybiz.dao.dao_binder import DaoBinder
from pybiz.api.middleware import ArgumentLoaderMiddleware

from .registry_decorator import RegistryDecorator
from .registry_proxy import RegistryProxy


class Registry(object):
    def __init__(self, middleware: List['RegistryMiddleware'] = None):
        self._decorators = []
        self._proxies = {}
        self._manifest = None
        self._is_bootstrapped = False
        self._is_started = False
        self._json_encoder = JsonEncoder()
        self._namespace = {}
        self._middleware = deque([
            m for m in (middleware or [])
            if isinstance(self, m.registry_types)
        ])

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

    @property
    def middleware(self) -> List['RegistryMiddleware']:
        return self._middleware

    @property
    def proxies(self) -> Dict[Text, RegistryProxy]:
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

    def register(self, proxy):
        self.proxies[proxy.name] = proxy

    def on_bootstrap(self, *args, **kwargs):
        pass

    def on_start(self):
        pass

    def bootstrap(
        self,
        manifest: Manifest = None,
        namespace: Dict = None,
        *args, **kwargs
    ):
        """
        Bootstrap the data, business, and service layers, wiring them up.
        """
        from pybiz import BizObject
        
        if self.is_bootstrapped:
            # if already bootstrapped, don't re-trigger all the base behavior
            # of this method. instead, only execute custom on_boostrap logic,
            # and make it the developer's responsiblity to make on_bootstrap
            # idempotent.
            self.on_bootstrap()
            return self

        # merge additional namespace data into namespace accumulator
        self._namespace = DictUtils.merge(self._namespace, namespace or {})

        # create, load, and process the manifest
        self._manifest = manifest or Manifest()
        self._manifest.load().process(namespace=self._namespace)

        for mware in self.middleware:
            mware.bootstrap(registry=self)

        # bootstrap the data access layer (DAL)
        binder = DaoBinder.get_instance()

        for binding in binder.bindings:
            strap = self.manifest.bootstraps.get(binding.dao_type_name)
            if strap is not None:
                binding.dao_type.bootstrap(**strap.params)
            else:
                binding.dao_type.bootstrap()

        binder.bind()

        # execute developer-provided custom logic
        self.on_bootstrap(*args, **kwargs)

        self._is_bootstrapped = True
        return self

    def start(self, *args, **kwargs):
        """
        Enter the main loop in whatever program context your Registry is
        being used, like in a web framework or a REPL.
        """
        self._is_started = True
        return self.on_start()

    def dump(self) -> Dict:
        """
        Return a Python dict that can be serialized to JSON, represents the
        contents of the Registry. The purpose of this method is to export
        metadata about this registry to be consumed by some other service or
        external process without said service or process needing to import this
        Registry directly.
        """
        return {
            'registry': {p.dump() for p in self.proxies.values()}
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
