import inspect
import logging
import os
import threading

from threading import local, get_ident
from typing import List, Dict, Text, Tuple, Set, Type, Callable, Union
from collections import deque, OrderedDict, namedtuple, defaultdict
from random import choice
from string import ascii_letters

from appyratus.utils.dict_utils import DictObject, DictUtils
from appyratus.utils.string_utils import StringUtils
from appyratus.logging import ConsoleLoggerInterface
from appyratus.enum import EnumValueStr
from appyratus.env import Environment

from ravel.manifest.manifest import Manifest
from ravel.util.json_encoder import JsonEncoder
from ravel.util.loggers import console
from ravel.util.misc_functions import get_class_name, inject, is_sequence
from ravel.schema import Field
from ravel.constants import ID
from ravel.app.exceptions import ApplicationError

from .action_decorator import ActionDecorator
from .action import Action
from .argument_loader import ArgumentLoader
from .resource_binder import ResourceBinder


class Mode(EnumValueStr):
    @staticmethod
    def values():
        return {
            'normal',
            'simulation',
        }


class Application(object):
    Mode = Mode

    def __init__(
        self,
        middleware: List['Middleware'] = None,
        manifest: Manifest = None,
        mode: Mode = Mode.normal,
    ):
        # thread-local storage
        self.local = local()
        self.local.is_bootstrapped = False
        self.local.is_started = False

        self.shared = DictObject()  # shared storage (WRT to threads)
        if manifest is not None:
            self.shared.manifest = Manifest(manifest)

        self.env = Environment()

        self._mode = mode
        self._actions = DictObject()
        self._namespace = {}
        self._json = JsonEncoder()
        self._arg_loader = None
        self._binder = ResourceBinder()
        self._logger = None

        self._middleware = deque([
            m for m in (middleware or [])
            if isinstance(self, m.app_types)
        ])


    def __getattr__(self, resource_class_name: Text) -> Type['Resource']:
        return self.res[resource_class_name]

    def __contains__(self, action: Text):
        return action in self.manifest.actions

    def __repr__(self):
        return (
            f'{get_class_name(self)}('
            f'bootstrapped={self.local.is_bootstrapped}, '
            f'started={self.local.is_started}, '
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
        if not self.is_bootstrapped:
            return self.decorator_type(self, *args, **kwargs)
        else:
            return self.start(*args, **kwargs)

    @property
    def decorator_type(self) -> Type[ActionDecorator]:
        return ActionDecorator

    @property
    def action_type(self) -> Type[Action]:
        return Action

    @property
    def manifest(self):
        return self.local.manifest

    @manifest.setter
    def manifest(self, obj):
        self._manifest = Manifest.from_object(obj)

    @property
    def mode(self) -> Mode:
        return self._mode

    @mode.setter
    def mode(self, mode):
        self._mode = Application.Mode(mode)

    @property
    def is_bootstrapped(self) -> bool:
        return self.local.is_bootstrapped

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
    def actions(self) -> DictObject:
        return self._actions

    @property
    def log(self):
        return self._logger

    def action(self, *args, **kwargs):
        return self.decorator_type(self, *args, **kwargs)

    def register(
        self,
        action: Union['Action', 'Application'],
        overwrite=False
    ) -> 'Application':
        """
        Add a Action to this app.
        """
        if isinstance(action, Application):
            app = action
            for act in app.actions.values():
                decorator = self.action()
                decorator(act.target)
        elif action.name not in self._actions or overwrite:
            console.debug(
                f'{get_class_name(self)}(thread_id={get_ident()}) '
                f'registered action {action.name}'
            )
            if isinstance(action, Action):
                if action.app is not self:
                    decorator(action.target)
                else:
                    self._actions[action.name] = action
            else:
                assert callable(action)
                decorator = self.action()
                decorator(action)
        else:
            raise ApplicationError(
                message=f'action already registered: {action.name}',
                data={'action': action}
            )

    def bind(
        self,
        resource_classes: List[Type['Resource']],
        store_class: Type['Store'] = None
    ) -> 'Application':
        """
        Dynamically register one or more Resource classes with this
        bootstrapped Application. If a store_class is specified, it will be used
        for all classes in resource_classes. Otherwise, the Store class will come from
        calling the __store__ method on each BizClass.
        """
        assert self.is_bootstrapped

        def bind_one(resource_class, store_class):
            store_instance = None
            if store_class is None:
                store_obj = resource_class.__store__()
                if not isinstance(store_obj, type):
                    store_class = type(store_obj)
                    store_instance = store_obj
                else:
                    store_class = store_obj

            if not resource_class.is_bootstrapped():
                resource_class.bootstrap(self)
            if not store_class.is_bootstrapped():
                kwargs = self.manifest.get_bootstrap_params(store_class)
                store_class.bootstrap(self, **kwargs)

            binding = self.binder.register(
                resource_class=resource_class,
                store_class=store_class,
                store_instance=store_instance,
            )
            binding.bind(self.binder)

        if not is_sequence(resource_classes):
            resource_classes = [resource_classes]

        for resource_class in resource_classes:
            bind_one(resource_class, store_class)

        self._arg_loader.bind()

        return self

    def bootstrap(
        self,
        manifest: Manifest = None,
        namespace: Dict = None,
        middleware: List = None,
        mode: Mode = Mode.normal,
        *args,
        **kwargs
    ) -> 'Application':
        """
        Bootstrap the data, business, and service layers, wiring them up.
        """
        if self.is_bootstrapped:
            console.warning(
                message=f'{get_class_name(self)} already bootstrapped.',
                data={
                    'pid': os.getpid(),
                    'thread': threading.get_ident(),
                }
            )
            return self

        console.debug(f'bootstrapping {get_class_name(self)} app')

        # override the application mode set in constructor
        if mode is not None:
            self._mode = Application.Mode(mode)

        # merge additional namespace data into namespace dict
        self._namespace = DictUtils.merge(self._namespace, namespace or {})

        if middleware:
            self._middleware.extend(
                m for m in middleware if isinstance(self, m.app_types)
            )

        if manifest is None and self.shared.manifest is not None:
            self.local.manifest = Manifest(self.shared.manifest)
        elif manifest is not None:
            self.local.manifest = Manifest(manifest)

        # manifest expected to have been passed into ctor
        assert self.local.manifest is not None

        # if this is the main process, use it's manifest as the shared copy.
        # this comes into play when bootstrap is re-called within the
        # initializer logic of a new thread.
        if not self.shared.manifest:
            self.shared.manifest = self.local.manifest

        # setup logger before bootstrapping components
        logger_suffix = ''.join(choice(ascii_letters) for i in range(4))
        logger_name = (
            self.manifest.package + '-' + logger_suffix if self.manifest.package
            else StringUtils.snake(get_class_name(self)) + '-' + logger_suffix
        )
        self._logger = ConsoleLoggerInterface(logger_name)

        self.manifest.bootstrap(self)
        # self.manifest.bind(rebind=True)

        # init the arg loader, which is responsible for replacing arguments
        # passed in as ID's with their respective Resources
        self._arg_loader = ArgumentLoader(self)

        # execute custom lifecycle hook provided by this subclass
        self.on_bootstrap(*args, **kwargs)
        self.local.is_bootstrapped = True

        console.debug(f'finished bootstrapping {get_class_name(self)}')
        return self

    def start(self, *args, **kwargs):
        """
        Enter the main loop in whatever program context your Application is
        being used, like in a web framework or a REPL.
        """
        self.local.is_started = True
        return self.on_start(*args, **kwargs)

    def inject(
        self,
        func: Callable,
        include_resource_classes=True,
        include_store_classes=True,
        include_actions=False,
    ) -> Callable:
        """
        Inject Resource, Store, and/or Action classes into the lexical scope of
        the given function.
        """
        if include_resource_classes:
            inject(func, self.manifest.resource_classes)
        if include_store_classes:
            inject(func, self.manifest.store_classes)
        if include_actions:
            inject(func, self.manifest.actions)

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

    def __init__(self, app: 'Application', store_classes: DictObject):
        self._app = app
        self._store_classes = DictObject(store_classes)

    @property
    def store_classes(self) -> DictObject:
        return self._store_classes

    @property
    def utilized_store_classes(self) -> Dict[Text, Type['Store']]:
        return {
            get_class_name(res_type.ravel.local.store): type(res_type.ravel.local.store)
            for res_type in self._app.res.values()
        }

    def bootstrap(self):
        for store_class in self.utilized_store_classes.values():
            kwargs = self._app.manifest.get_bootstrap_params(store_class)
            store_class.bootstrap(self._app, **kwargs)
