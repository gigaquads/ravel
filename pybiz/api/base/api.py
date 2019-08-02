import inspect
import logging

from typing import List, Dict, Text, Tuple, Set, Type
from collections import deque

from appyratus.utils import DictObject, DictUtils

from pybiz.manifest import Manifest
from pybiz.util.json_encoder import JsonEncoder
from pybiz.util.loggers import console

from ..exc import ApiError
from .api_decorator import ApiDecorator
from .api_proxy import Proxy
from .api_argument_loader import ApiArgumentLoader


class Api(object):
    def __init__(self, middleware: List['ApiMiddleware'] = None):
        self._decorators = []
        self._proxies = {}
        self._api = DictObject(self._proxies)
        self._manifest = None
        self._is_bootstrapped = False
        self._is_started = False
        self._json_encoder = JsonEncoder()
        self._namespace = {}
        self._arg_loader = None
        self._middleware = deque([
            m for m in (middleware or [])
            if isinstance(self, m.api_types)
        ])

    def __repr__(self):
        return (
            f'<{self.__class__.__name__}('
            f'bootstrapped={self._is_bootstrapped}, '
            f'started={self._is_started}, '
            f'size={len(self._proxies)}'
            f')>'
        )

    def __call__(self, *args, **kwargs) -> ApiDecorator:
        """
        Use this to decorate functions, adding them to this Api.
        Each time a function is decorated, it arives at the "on_decorate"
        method, where you can api the function with a web framework or
        whatever system you have in mind.

        Usage:

        ```python3
            api = Api()

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
    def decorator_type(self) -> Type[ApiDecorator]:
        return ApiDecorator

    @property
    def proxy_type(self) -> Type[Proxy]:
        return Proxy

    @property
    def manifest(self) -> Manifest:
        return self._manifest

    @property
    def middleware(self) -> List['ApiMiddleware']:
        return self._middleware

    @property
    def proxies(self) -> Dict[Text, Proxy]:
        return self._proxies

    @property
    def decorators(self) -> List[ApiDecorator]:
        return self._decorators

    @property
    def argument_loader(self):
        return self._arg_loader

    @property
    def types(self) -> DictObject:
        return self._manifest.types

    @property
    def api(self) -> DictObject:
        return self._api

    @property
    def is_bootstrapped(self):
        return self._is_bootstrapped

    def register(self, proxy):
        """
        Add a Proxy to this api.
        """
        if proxy.name not in self._proxies:
            console.debug(
                f'registering "{proxy.name}" with '
                f'{self.__class__.__name__}...'
            )
            self._proxies[proxy.name] = proxy
        else:
            raise ApiError(
                message=f'proxy already registered, {proxy.name}',
                data={'proxy': proxy}
            )

    def bootstrap(
        self,
        manifest: Manifest = None,
        namespace: Dict = None,
        *args, **kwargs
    ):
        """
        Bootstrap the data, business, and service layers, wiring them up.
        """
        if self.is_bootstrapped:
            console.warning(f'{self} already bootstrapped. skipping...')
            return self

        console.debug(f'bootstrapping "{self.__class__.__name__}" Api...')

        # merge additional namespace data into namespace accumulator
        self._namespace = DictUtils.merge(self._namespace, namespace or {})

        # create, load, and process the manifest
        # bootstrap the biz and data access layers, and
        # bind each BizObject class with its Dao object.
        if manifest is None:
            self._manifest = Manifest()
        elif isinstance(manifest, str):
            self._manifest = Manifest(path=manifest)
        elif isinstance(manifest, dict):
            self._manifest = Manifest(data=manifest)
        else:
            self._manifest = manifest

        self._manifest.load()
        self._manifest.process(namespace=self._namespace)
        self._manifest.bootstrap(api=self)
        self._manifest.bind()

        # bootstrap the middlware
        for mware in self.middleware:
            console.debug(f'bootstrapping {mware}')
            mware.bootstrap(api=self)

        # init the arg loader, which is responsible for replacing arguments
        # passed in as ID's with their respective BizObjects
        self._arg_loader = ApiArgumentLoader(self)

        # execute custom lifecycle hook provided by this subclass
        self.on_bootstrap(*args, **kwargs)
        self._is_bootstrapped = True

        console.debug(f'finished bootstrapping {self.__class__.__name__}')

        return self

    def start(self, *args, **kwargs):
        """
        Enter the main loop in whatever program context your Api is
        being used, like in a web framework or a REPL.
        """
        console.info(f'starting {self.__class__.__name__}...')
        self._is_started = True
        return self.on_start()

    def on_bootstrap(self, *args, **kwargs):
        pass

    def on_decorate(self, proxy: 'Proxy'):
        """
        We come here whenever a function is decorated by this api. Here we
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

    def on_start(self):
        pass
