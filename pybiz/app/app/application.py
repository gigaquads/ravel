import inspect
import logging

from typing import List, Dict, Text, Tuple, Set, Type, Callable
from collections import deque, OrderedDict, namedtuple

from appyratus.utils import DictObject, DictUtils

from pybiz.manifest import Manifest
from pybiz.util.json_encoder import JsonEncoder
from pybiz.util.loggers import console
from pybiz.util.misc_functions import get_class_name, inject, is_sequence
from pybiz.schema import Field, UuidString
from pybiz.constants import ID_FIELD_NAME

from .exceptions import ApplicationError
from .endpoint_decorator import EndpointDecorator
from .endpoint import Endpoint
from .application_argument_loader import ArgumentLoader
from .resource_binder import ResourceBinder

DEFAULT_ID_FIELD_CLASS = UuidString


class Application(object):

    def __init__(
        self,
        middleware: List['Middleware'] = None,
    ):
        self._state = DictObject()
        self._decorators = []
        self._endpoints = {}
        self._biz = DictObject()
        self._dal = DictObject()
        self._api = DictObject()
        self._manifest = None  # set in bootstrap
        self._arg_loader = None  # set in bootstrap
        self._is_bootstrapped = False
        self._is_started = False
        self._namespace = {}
        self._json_encoder = JsonEncoder()
        self._binder = ResourceBinder()
        self._middleware = deque([
            m for m in (middleware or [])
            if isinstance(self, m.app_types)
        ])

    def __contains__(self, endpoint_name: Text):
        return endpoint_name in self._endpoints

    def __repr__(self):
        return (
            f'{get_class_name(self)}('
            f'bootstrapped={self._is_bootstrapped}, '
            f'started={self._is_started}, '
            f'size={len(self._endpoints)}'
            f')'
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
    def manifest(self) -> Manifest:
        return self._manifest

    @property
    def state(self) -> DictObject:
        return self._state

    @property
    def middleware(self) -> List['Middleware']:
        return self._middleware

    @property
    def endpoints(self) -> Dict[Text, Endpoint]:
        return self._endpoints

    @property
    def decorators(self) -> List[EndpointDecorator]:
        return self._decorators

    @property
    def loader(self) -> 'ArgumentLoader':
        return self._arg_loader

    @property
    def binder(self) -> 'ResourceBinder':
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
    def is_bootstrapped(self) -> bool:
        return self._is_bootstrapped

    def register(self, endpoint: 'Endpoint') -> 'Application':
        """
        Add a Endpoint to this app.
        """
        if endpoint.name not in self._endpoints:
            console.debug(
                f'registering endpoint "{endpoint.name}" with '
                f'{get_class_name(self)}...'
            )
            self._endpoints[endpoint.name] = endpoint
            self._api[endpoint.name] = endpoint
        else:
            raise ApplicationError(
                message=f'endpoint already registered, {endpoint.name}',
                data={'endpoint': endpoint}
            )

    def bind(
        self,
        biz_classes: List[Type['Resource']],
        store_class: Type['Store'] = None
    ) -> 'Application':
        """
        Dynamically register one or more Resource classes with this
        bootstrapped Application. If a store_class is specified, it will be used
        for all classes in biz_classes. Otherwise, the Store class will come from
        calling the __store__ method on each BizClass.
        """
        assert self.is_bootstrapped

        def bind_one(biz_class, store_class):
            store_instance = None
            if store_class is None:
                store_obj = biz_class.__store__()
                if not isinstance(store_obj, type):
                    store_class = type(store_obj)
                    store_instance = store_obj
                else:
                    store_class = store_obj

            self._dal[get_class_name(store_class)] = store_class
            self._biz[get_class_name(biz_class)] = biz_class

            if not biz_class.is_bootstrapped():
                biz_class.bootstrap(self)
            if not store_class.is_bootstrapped():
                store_class.bootstrap(self)

            binding = self.binder.register(
                biz_class=biz_class,
                store_class=store_class,
                store_instance=store_instance,
            )
            binding.bind(self.binder)

        if not is_sequence(biz_classes):
            biz_classes = [biz_classes]

        for biz_class in biz_classes:
            bind_one(biz_class, store_class)

        self._arg_loader.bind()

        return self

    def bootstrap(
        self,
        manifest: Manifest = None,
        namespace: Dict = None,
        middleware: List = None,
        *args,
        **kwargs
    ) -> 'Application':
        """
        Bootstrap the data, business, and service layers, wiring them up.
        """
        if self.is_bootstrapped:
            console.warning(f'{self} already bootstrapped. skipping...')

        console.debug(f'bootstrapping "{get_class_name(self)}" Application...')

        if middleware:
            self._middleware.extend(
                m for m in middleware if isinstance(self, m.app_types)
            )

        # merge additional namespace data into namespace accumulator
        self._namespace = DictUtils.merge(self._namespace, namespace or {})

        # create, load, and process the manifest
        # bootstrap the biz and data access layers, and
        # bind each Resource class with its Store object.
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

        self._biz.update(self._manifest.types.biz)
        self._dal.update(self._manifest.types.dal)
        self._api.update(self._endpoints)

        self._manifest.bootstrap()
        self._manifest.bind(rebind=True)

        # bootstrap the middlware
        for mware in self.middleware:
            mware.bootstrap(app=self)

        # execute custom lifecycle hook provided by this subclass
        self.on_bootstrap(*args, **kwargs)

        self._is_bootstrapped = True

        # init the arg loader, which is responsible for replacing arguments
        # passed in as ID's with their respective Resources
        self._arg_loader = ArgumentLoader(self)

        console.debug(f'finished bootstrapping application')

        return self

    def start(self, *args, **kwargs):
        """
        Enter the main loop in whatever program context your Application is
        being used, like in a web framework or a REPL.
        """
        console.info(f'starting {get_class_name(self)}...')
        self._is_started = True
        return self.on_start()

    def inject(self, func: Callable, biz=True, dal=True, api=True) -> Callable:
        """
        Inject Resource, Store, and/or Endpoint classes into the lexical scope of
        the given function.
        """
        if biz:
            inject(func, self.biz)
        if dal:
            inject(func, self.dal)
        if api:
            inject(func, self.api)

        return func

    def on_extract(self, endpoint, index, parameter, raw_args, raw_kwargs):
        """
        Return a value for the given the name of the `inspect` module's
        parameter object. This is used by on_request when extracting endpoint
        arguments.
        """
        if index is not None:
            return raw_args[index]
        else:
            return raw_kwargs.get(parameter.name)

    def on_bootstrap(self, *args, **kwargs):
        """
        Developer logic to execute upon calling app.bootstrap.
        """

    def on_decorate(self, endpoint: 'Endpoint'):
        """
        We come here whenever a function is decorated by this app. Here we
        can add the decorated function to, say, a web framework as a route.
        """

    def on_start(self) -> object:
        """
        Custom logic to perform when app.start is called. The return value from
        on_start is used at the return value of start.
        """
        return None

    def on_request(
        self,
        endpoint: 'Endpoint',
        *raw_args,
        **raw_kwargs
    ) -> Tuple[Tuple, Dict]:
        """
        This executes immediately before calling a registered function. You
        must return re-packaged args and kwargs here. However, if nothing is
        returned, the raw args and kwargs are used.
        """
        args_dict = OrderedDict()
        kwargs = {}

        for idx, param in enumerate(endpoint.signature.parameters.values()):
            target_dict = None
            index = None
            if param.kind == inspect.Parameter.POSITIONAL_OR_KEYWORD:
                if param.default is inspect._empty:
                    target_dict = args_dict
                    index = idx
                else:
                    target_dict = kwargs
            elif param.kind == inspect.Parameter.POSITIONAL_ONLY:
                target_dict = args_dict
                index = idx
            elif param.kind == inspect.Parameter.KEYWORD_ONLY:
                target_dict = kwargs

            if target_dict is not None:
                target_dict[param.name] = self.on_extract(
                    endpoint, index, param, raw_args, raw_kwargs
                )

        # transform positional args dict into a named tuple
        PositionalArguments = namedtuple(
            typename='PositionalArguments', field_names=args_dict.keys()
        )
        args = PositionalArguments(*tuple(args_dict.values()))

        return (args, kwargs)

    def on_response(
        self,
        endpoint: 'Endpoint',
        raw_result: object,
        *raw_args,
        **raw_kwargs
    ) -> object:
        """
        The return value of registered callables come here as `result`. Here
        any global post-processing can be done. Args and kwargs consists of
        whatever raw data was passed into the callable *before* on_request
        executed.
        """
        return raw_result
