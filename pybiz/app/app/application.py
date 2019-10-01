import inspect
import logging

from typing import List, Dict, Text, Tuple, Set, Type
from collections import deque

from appyratus.utils import DictObject, DictUtils

from pybiz.manifest import Manifest
from pybiz.util.json_encoder import JsonEncoder
from pybiz.util.loggers import console
from pybiz.schema import Field, UuidString

from .exceptions import ApplicationError
from .endpoint_decorator import EndpointDecorator
from .endpoint import Endpoint
from .application_argument_loader import ApplicationArgumentLoader
from .application_dao_binder import ApplicationDaoBinder

DEFAULT_ID_FIELD_CLASS = UuidString


class Application(object):

    def __init__(
        self,
        middleware: List['ApplicationMiddleware'] = None,
        id_field_class: Type[Field] = None,
    ):
        self._id_field_class = id_field_class or DEFAULT_ID_FIELD_CLASS
        self._decorators = []
        self._endpoints = {}
        self._biz = None  # set in bootstrap
        self._dal = None  # set in bootstrap
        self._api = None  # set in bootstrap
        self._manifest = None  # set in bootstrap
        self._arg_loader = None  # set in bootstrap
        self._is_bootstrapped = False
        self._is_started = False
        self._json_encoder = JsonEncoder()
        self._namespace = {}
        self._binder = ApplicationDaoBinder()
        self._middleware = deque([
            m for m in (middleware or [])
            if isinstance(self, m.app_types)
        ])

    def __contains__(self, endpoint_name: Text):
        return endpoint_name in self._endpoints

    def __repr__(self):
        return (
            f'<{self.__class__.__name__}('
            f'bootstrapped={self._is_bootstrapped}, '
            f'started={self._is_started}, '
            f'size={len(self._endpoints)}'
            f')>'
        )

    def __call__(self, *args, **kwargs) -> EndpointDecorator:
        """
        Use this to decorate functions, adding them to this Application.
        Each time a function is decorated, it arives at the "on_decorate"
        method, where you can app the function with a web framework or
        whatever system you have in mind.

        Usage:

        ```python3
            app = Application()

            @app()
            def do_something():
                pass
        ```
        """
        # build and the decorator
        decorator = self.decorator_class(self, *args, **kwargs)
        self.decorators.append(decorator)
        return decorator

    @property
    def decorator_class(self) -> Type[EndpointDecorator]:
        return EndpointDecorator

    @property
    def endpoint_class(self) -> Type[Endpoint]:
        return Endpoint

    @property
    def id_field_class(self) -> Type[Field]:
        return self._id_field_class

    @property
    def manifest(self) -> Manifest:
        return self._manifest

    @property
    def middleware(self) -> List['ApplicationMiddleware']:
        return self._middleware

    @property
    def endpoints(self) -> Dict[Text, Endpoint]:
        return self._endpoints

    @property
    def decorators(self) -> List[EndpointDecorator]:
        return self._decorators

    @property
    def loader(self) -> 'ApplicationArgumentLoader':
        return self._arg_loader

    @property
    def binder(self) -> 'ApplicationDaoBinder':
        return self._binder

    @property
    def types(self) -> DictObject:
        return self._manifest.types

    @property
    def biz(self) -> DictObject:
        return self._biz

    @property
    def api(self) -> DictObject:
        return self._api

    @property
    def dal(self) -> DictObject:
        return self._dal

    @property
    def is_bootstrapped(self):
        return self._is_bootstrapped

    def register(self, endpoint):
        """
        Add a Endpoint to this app.
        """
        if endpoint.name not in self._endpoints:
            console.debug(
                f'registering "{endpoint.name}" with '
                f'{self.__class__.__name__}...'
            )
            self._endpoints[endpoint.name] = endpoint
        else:
            raise ApplicationError(
                message=f'endpoint already registered, {endpoint.name}',
                data={'endpoint': endpoint}
            )

    def bootstrap(
        self,
        manifest: Manifest = None,
        namespace: Dict = None,
        rebootstrap: bool = False,
        *args, **kwargs
    ):
        """
        Bootstrap the data, business, and service layers, wiring them up.
        """
        if self.is_bootstrapped and not rebootstrap:
            console.warning(f'{self} already bootstrapped. skipping...')
            return self

        console.debug(f'bootstrapping "{self.__class__.__name__}" Application...')

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
        self._manifest.process(app=self, namespace=self._namespace)

        self._biz = DictObject(self._manifest.types.biz)
        self._dal = DictObject(self._manifest.types.dal)
        self._api = DictObject(self._endpoints)

        self._manifest.bootstrap()
        self._manifest.bind(rebind=rebootstrap)

        # bootstrap the middlware
        for mware in self.middleware:
            console.debug(f'bootstrapping {mware}')
            mware.bootstrap(app=self)

        # execute custom lifecycle hook provided by this subclass
        self.on_bootstrap(*args, **kwargs)

        self._is_bootstrapped = True

        # init the arg loader, which is responsible for replacing arguments
        # passed in as ID's with their respective BizObjects
        self._arg_loader = ApplicationArgumentLoader(self)

        console.debug(f'finished bootstrapping {self.__class__.__name__}')

        return self

    def start(self, *args, **kwargs):
        """
        Enter the main loop in whatever program context your Application is
        being used, like in a web framework or a REPL.
        """
        console.info(f'starting {self.__class__.__name__}...')
        self._is_started = True
        return self.on_start()

    def on_bootstrap(self, *args, **kwargs):
        pass

    def on_decorate(self, endpoint: 'Endpoint'):
        """
        We come here whenever a function is decorated by this app. Here we
        can add the decorated function to, say, a web framework as a route.
        """

    def on_request(self, endpoint, *args, **kwargs) -> Tuple[Tuple, Dict]:
        """
        This executes immediately before calling a registered function. You
        must return re-packaged args and kwargs here. However, if nothing is
        returned, the raw args and kwargs are used.
        """
        return (args, kwargs)

    def on_response(self, endpoint, result, *args, **kwargs) -> object:
        """
        The return value of registered callables come here as `result`. Here
        any global post-processing can be done. Args and kwargs consists of
        whatever raw data was passed into the callable *before* on_request
        executed.
        """
        return result

    def on_start(self):
        pass
