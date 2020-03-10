import inspect
import logging

from threading import local
from typing import List, Dict, Text, Tuple, Set, Type, Callable, Union
from collections import deque, OrderedDict, namedtuple

from appyratus.utils import DictObject, DictUtils
from appyratus.enum import EnumValueStr

from ravel.manifest import Manifest
from ravel.util.json_encoder import JsonEncoder
from ravel.util.loggers import console
from ravel.util.misc_functions import get_class_name, inject, is_sequence
from ravel.schema import Field, UuidString
from ravel.constants import ID
from ravel.app.exceptions import ApplicationError

from .action_decorator import ActionDecorator
from .action import Action
from .argument_loader import ArgumentLoader
from .resource_binder import ResourceBinder


class Application(object):

    class Mode(EnumValueStr):
        @staticmethod
        def values():
            return {
                'normal',
                'simulation',
            }


    def __init__(
        self,
        middleware: List['Middleware'] = None,
        manifest: Manifest = None,
        mode: Mode = Mode.normal,
    ):
        self.manifest = Manifest.from_object(manifest)
        self.local = local()

        self._mode = mode
        self._res = DictObject()
        self._storage = None
        self._actions = DictObject()
        self._namespace = {}
        self._json = JsonEncoder()
        self._arg_loader = None
        self._binder = ResourceBinder()

        self._middleware = deque([
            m for m in (middleware or [])
            if isinstance(self, m.app_types)
        ])

        self._is_bootstrapped = False
        self._is_started = False

    def __contains__(self, action: Text):
        return action in self._actions

    def __repr__(self):
        return (
            f'{get_class_name(self)}('
            f'bootstrapped={self._is_bootstrapped}, '
            f'started={self._is_started}, '
            f'size={len(self._actions)}'
            f')'
        )

    def __call__(
        self, *args, **kwargs
    ) ->Union[ActionDecorator, List[ActionDecorator]]:
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
        return self.decorator_type(self, *args, **kwargs)

    @property
    def decorator_type(self) -> Type[ActionDecorator]:
        return ActionDecorator

    @property
    def action_type(self) -> Type[Action]:
        return Action

    @property
    def mode(self) -> Mode:
        return self._mode

    @mode.setter
    def mode(self, mode):
        self._mode = Application.Mode(mode)

    @property
    def is_bootstrapped(self) -> bool:
        return self._is_bootstrapped

    @property
    def is_simulation(self) -> bool:
        return self._mode == Application.Mode.simulation

    @is_simulation.setter
    def is_simulation(self, is_simulation):
        if is_simulation:
            self._mode = Application.Mode.simulation
        else:
            self._mode = Application.Mode.normal

    @property
    def json(self) -> JsonEncoder:
        return self._json

    @property
    def middleware(self) -> List['Middleware']:
        return self._middleware

    @property
    def loader(self) -> 'ArgumentLoader':
        return self._arg_loader

    @property
    def binder(self) -> 'ResourceBinder':
        return self._binder

    @property
    def res(self) -> DictObject:
        return self._res

    @property
    def actions(self) -> DictObject:
        return self._actions

    @property
    def storage(self) -> 'StoreManager':
        return self._storage

    def register(self, action: 'Action', overwrite=False) -> 'Application':
        """
        Add a Action to this app.
        """
        if action.name not in self._actions or overwrite:
            console.debug(
                f'registering {action.name} action with '
                f'{get_class_name(self)}...'
            )
            self._actions[action.name] = action
        else:
            raise ApplicationError(
                message=f'action already registered: {action.name}',
                data={'action': action}
            )

    def bind(
        self,
        resource_types: List[Type['Resource']],
        store_type: Type['Store'] = None
    ) -> 'Application':
        """
        Dynamically register one or more Resource classes with this
        bootstrapped Application. If a store_type is specified, it will be used
        for all classes in resource_types. Otherwise, the Store class will come from
        calling the __store__ method on each BizClass.
        """
        assert self.is_bootstrapped

        def bind_one(resource_type, store_type):
            store_instance = None
            if store_type is None:
                store_obj = resource_type.__store__()
                if not isinstance(store_obj, type):
                    store_type = type(store_obj)
                    store_instance = store_obj
                else:
                    store_type = store_obj

            self._storage.store_types[get_class_name(store_type)] = store_type
            self._res[get_class_name(resource_type)] = resource_type

            if not resource_type.is_bootstrapped():
                resource_type.bootstrap(self)
            if not store_type.is_bootstrapped():
                kwargs = self.manifest.get_bootstrap_params(store_type)
                store_type.bootstrap(self, **kwargs)

            binding = self.binder.register(
                resource_type=resource_type,
                store_type=store_type,
                store_instance=store_instance,
            )
            binding.bind(self.binder)

        if not is_sequence(resource_types):
            resource_types = [resource_types]

        for resource_type in resource_types:
            bind_one(resource_type, store_type)

        self._arg_loader.bind()

        return self

    def bootstrap(
        self,
        manifest: Manifest = None,
        namespace: Dict = None,
        middleware: List = None,
        force=False,
        *args,
        **kwargs
    ) -> 'Application':
        """
        Bootstrap the data, business, and service layers, wiring them up.
        """
        if self.is_bootstrapped and (not force):
            console.warning(
                f'{get_class_name(self)} already bootstrapped. '
                f'skipping'
            )
            return self

        console.debug(f'bootstrapping {get_class_name(self)} application')

        # merge additional namespace data into namespace accumulator
        self._namespace = DictUtils.merge(self._namespace, namespace or {})

        if middleware:
            self._middleware.extend(
                m for m in middleware if isinstance(self, m.app_types)
            )

        if manifest is not None:
            self.manifest = Manifest.from_object(manifest)
        else:
            # manifest expected to have been passed into ctor
            assert self.manifest is not None

        self.manifest.process(app=self, namespace=self._namespace)

        self._storage = StoreManager(self, self.manifest.types.stores)
        self._res.update(self.manifest.types.res)

        self.manifest.bootstrap()
        self.manifest.bind(rebind=True)

        # init the arg loader, which is responsible for replacing arguments
        # passed in as ID's with their respective Resources
        self._arg_loader = ArgumentLoader(self)

        # bootstrap the middlware
        for mware in self.middleware:
            mware.bootstrap(app=self)

        for action in self._actions.values():
            action.bootstrap()

        # execute custom lifecycle hook provided by this subclass
        self.on_bootstrap(*args, **kwargs)
        self._is_bootstrapped = True

        console.debug(f'finished bootstrapping {get_class_name(self)}')
        return self

    def start(self, *args, **kwargs):
        """
        Enter the main loop in whatever program context your Application is
        being used, like in a web framework or a REPL.
        """
        self._is_started = True
        return self.on_start(*args, **kwargs)

    def inject(
        self,
        func: Callable,
        include_resource_types=True,
        include_store_types=True,
        include_actions=False
    ) -> Callable:
        """
        Inject Resource, Store, and/or Action classes into the lexical scope of
        the given function.
        """
        if include_resource_types:
            inject(func, self.res)
        if include_store_types:
            inject(func, self.storage.store_types)
        if include_actions:
            inject(func, self.actions)

        return func

    def on_extract(self, action, index, parameter, raw_args, raw_kwargs):
        """
        Return a value for the given the name of the `inspect` module's
        parameter object. This is used by on_request when extracting action
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

    def on_decorate(self, action: 'Action'):
        """
        We come here whenever a function is decorated by this app. Here we
        can add the decorated function to, say, a web framework as a route.
        """

    def on_start(self, *args, **kwargs) -> object:
        """
        Custom logic to perform when app.start is called. The return value from
        on_start is used at the return value of start.
        """
        return None

    def on_request(
        self,
        action: 'Action',
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

        for idx, param in enumerate(action.signature.parameters.values()):
            if not idx:
                # idx 0 is for the Request object injected elsewhere
                continue

            target_dict = None
            index = None
            if param.kind == inspect.Parameter.POSITIONAL_OR_KEYWORD:
                if param.default is inspect._empty:
                    target_dict = args_dict
                    index = idx - 1  # -1 because of injected "request" arg
                else:
                    target_dict = kwargs
            elif param.kind == inspect.Parameter.POSITIONAL_ONLY:
                target_dict = args_dict
                index = idx
            elif param.kind == inspect.Parameter.KEYWORD_ONLY:
                target_dict = kwargs

            if target_dict is not None:
                target_dict[param.name] = self.on_extract(
                    action, index, param, raw_args, raw_kwargs
                )

        # transform positional args dict into a named tuple
        PositionalArguments = namedtuple(
            typename='PositionalArguments', field_names=args_dict.keys()
        )
        args = PositionalArguments(*tuple(args_dict.values()))

        return (args, kwargs)

    def on_response(
        self,
        action: 'Action',
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


# TODO: move this class into ravel/app/base/store_manager.py
class StoreManager(object):
    """
    StoreManager is a high-level interface to performing logic related to the
    lifecycle of store instances utilized in a bootstrapped app.
    """

    def __init__(self, app: 'Application', store_types: DictObject):
        self._app = app
        self._store_types = store_types

    @property
    def store_types(self) -> DictObject:
        return self._store_types

    @property
    def utilized_store_types(self) -> Dict[Text, Type['Store']]:
        return {
            get_class_name(res_type.ravel.store): type(res_type.ravel.store)
            for res_type in self._app.res.values()
        }

    def bootstrap(self):
        for store_type in self.utilized_store_types.values():
            kwargs = self._app.manifest.get_bootstrap_params(store_type)
            store_type.bootstrap(self._app, **kwargs)
