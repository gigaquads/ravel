import inspect
import threading
import os

from threading import local, get_ident, current_thread
from concurrent.futures import (
    ThreadPoolExecutor, ProcessPoolExecutor, Future
)
from typing import (
    List, Dict, Text, Tuple, Type, Callable, Union, Optional
)
from collections import deque, OrderedDict, namedtuple

from appyratus.utils.dict_utils import DictObject, DictUtils
from appyratus.utils.string_utils import StringUtils
from appyratus.enum import EnumValueStr
from appyratus.env import Environment

from ravel.manifest.manifest import Manifest
from ravel.logging import ConsoleLoggerInterface
from ravel.util.json_encoder import JsonEncoder
from ravel.util.loggers import console
from ravel.util.misc_functions import get_class_name, inject
from ravel.constants import ID
from ravel.app.exceptions import ApplicationError
from ravel.store.transaction_manager import TransactionManager

from .action import Action
from .action_decorator import ActionDecorator
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
        self.local.manifest = Manifest(manifest) if manifest else None
        self.local.bootstrapper_thread_id = None
        self.local.thread_executor = None
        self.local.process_executor = None
        self.local.tx_manager = None

        self.shared = DictObject()  # storage shared by threads
        self.shared.manifest_data = None
        self.shared.app_counter = 1

        self.env = Environment()
        self._mode = mode
        self._json = JsonEncoder()
        self._binder = ResourceBinder()
        self._namespace = {}
        self._arg_loader = None
        self._logger = None
        self._root_pid = os.getpid()
        self._actions = DictObject()

        self._middleware = deque([
            m for m in (middleware or [])
            if isinstance(self, m.app_types)
        ])

        # set default main thread name
        current_thread().name = (
            f'{get_class_name(self)}MainThread'
        )


    def __getattr__(
        self,
        class_name: Text
    ) -> Optional[Union[Type['Resource'], Type['Store']]]:
        """
        This makes it possible to do app.ResourceClass or app.StoreClass as a
        convenient way of accessing registered store and resoruce classes at
        runtime--as opposed to importing them and risking annoying cyclic
        import errors.
        """
        class_obj = None
        if self.manifest is not None:
            # assume it's the name of resource class first
            class_obj = self.manifest.resource_classes.get(class_name)
            if not class_obj:
                # fall back on assuming it's a store class
                class_obj = self.manifest.store_classes.get(class_name)
        return class_obj

    def __getitem__(
        self,
        class_name: Text
    ) -> Union[Type['Resource'], Type['Store']]:
        return getattr(self, class_name, None)

    def __repr__(self):
        return (
            f'{get_class_name(self)}('
            f'bootstrapped={self.is_bootstrapped}, '
            f'started={self.is_started}, '
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
    def manifest(self) -> 'Manifest':
        return getattr(self.local, 'manifest', None)

    @manifest.setter
    def manifest(self, manifest):
        self.local.manifest = Manifest(manifest)

    @property
    def values(self) -> Dict:
        return self.manifest.values

    @property
    def mode(self) -> Mode:
        return self._mode

    @mode.setter
    def mode(self, mode):
        self._mode = Mode(mode)

    @property
    def namespace(self) -> Dict:
        return self._namespace

    @property
    def is_bootstrapped(self) -> bool:
        return getattr(self.local, 'is_bootstrapped', False)

    @property
    def is_started(self) -> bool:
        return getattr(self.local, 'is_started', False)

    @property
    def is_simulation(self) -> bool:
        return self._mode == Mode.simulation

    @is_simulation.setter
    def is_simulation(self, is_simulation):
        if is_simulation:
            self._mode = Mode.simulation
        else:
            self._mode = Mode.normal

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
    def actions(self) -> DictObject:
        return self._actions

    @property
    def log(self) -> ConsoleLoggerInterface:
        return self._logger

    def action(self, *args, **kwargs) -> 'ActionDecorator':
        return self.decorator_type(self, *args, **kwargs)

    def bootstrap(
        self,
        manifest: Union[Dict, Manifest, Text, Callable, 'Application'] = None,
        namespace: Dict = None,
        values: Dict = None,
        middleware: List = None,
        mode: Mode = Mode.normal,
        *args,
        **kwargs
    ) -> 'Application':
        """
        Bootstrap the data, business, and service layers, wiring them up.
        """

        def create_logger(level):
            """
            Setup root application logger.
            """
            if self.manifest.package:
                name = (
                    f'{self.manifest.package}:'
                    f'{os.getpid()}:'
                    f'{get_ident()}'
                )
            else:
                class_name = get_class_name(self)
                name = (
                    f'{StringUtils.snake(class_name)}:'
                    f'{os.getpid()}:'
                    f'{get_ident()}'
                )

            return ConsoleLoggerInterface(name, level)
            
        # warn about already being bootstrapped...
        if (
            self.is_bootstrapped and
            self.local.thread_id != get_ident()
        ):
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
        self._mode = Mode(mode or self.mode or Mode.normal)

        # merge additional namespace data into namespace dict
        self._namespace = DictUtils.merge(self._namespace, namespace or {})

        # register additional middleware targing this app subclass
        if middleware:
            self._middleware.extend(
                m for m in middleware if isinstance(self, m.app_types)
            )

        # build final manifest, used to bootstrap program components
        if manifest is not None:
            if isinstance(manifest, Application):
                source_app = manifest
                self.local.manifest = Manifest(source_app.shared.manifest_data)
            else:
                self.local.manifest = Manifest(manifest)
        elif self.shared.manifest_data:
            self.local.manifest = Manifest(self.shared.manifest_data)
        else:
            self.local.manifest = Manifest({})

        assert self.local.manifest is not None
        self.manifest.bootstrap(self)

        if values:
            self.manifest.values.update(values)
            
        if not self.shared.manifest_data:
            self.shared.manifest_data = self.local.manifest.data

        # set up main thread name
        if self.manifest.package:
            current_thread().name = (
                f'{StringUtils.camel(self.manifest.package)}'
            )
        else:
            # update default main thread name
            current_thread().name = (
                f'{get_class_name(self)}'
            )

        self._logger = create_logger(self.manifest.logging['level'])
        self._arg_loader = ArgumentLoader(self)

        # if we're in a new process, unset the executors so that
        # the spawn method lazily triggers the instantiation of
        # new ones at runtime.
        if not self._root_pid != os.getpid():
            self.local.thread_executor = None
            self.local.process_executor = None

        self.local.thread_id = get_ident()
        self.local.tx_manager = TransactionManager(self)

        # execute custom lifecycle hook provided by this subclass
        self.on_bootstrap(*args, **kwargs)
        self.local.is_bootstrapped = True

        console.debug(f'finished bootstrapping {get_class_name(self)} app')
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
            inject(func, self._actions)

        return func

    def spawn(
        self,
        target: Callable,
        args: Tuple = None,
        kwargs: Dict = None,
        multiprocessing: bool = False
    ) -> Future:
        """
        Perform a target callable in a worker thread or subprocess, creating
        and returning a Future object. This is a convenience method;
        otherwise, when you manually create a thread of process in which you
        intend to access this app, you will need to call bootstrap on the app
        again (within said thread or process context).
        """
        args = tuple(args or [])
        kwargs = kwargs or {}

        # select and lazily create the appropriate executor...
        if not multiprocessing:
            executor = getattr(self.local, 'thread_executor', None)
            if executor is None:
                thread_name_prefix = (
                    f'{StringUtils.camel(self.manifest.package)} Worker'
                    if self.manifest.package
                    else f'{get_class_name(self)} Worker'
                )
                self.local.thread_executor = ThreadPoolExecutor(
                    max_workers=os.cpu_count(),
                    initializer=self.bootstrap,
                    initargs=(self.manifest.data, ),
                    thread_name_prefix=thread_name_prefix
                )
            executor = self.local.thread_executor
        else:
            executor = getattr(self.local, 'process_executor', None)
            if executor is None:
                self.local.process_executor = ProcessPoolExecutor(
                    max_workers=os.cpu_count(),
                    initializer=self.bootstrap,
                    initargs=(self.manifest.data, ),
                )
            executor = self.local.process_executor

        # submit and return the future
        return executor.submit(target, *args, **kwargs)

    def add_action(self, action: 'Action', overwrite=False, *args, **kwargs):
        """
        Add an action to this app.
        """
        if action.name not in self._actions or overwrite:
            console.debug(f'registered action {action.name}')
            if isinstance(action, Action):
                if action.app is not self:
                    if args or kwargs:
                        console.warning(
                            '*args and **kwargs ignored by add_action'
                        )
                    decorator = self.action(
                        *action.decorator.args,
                        **action.decorator.kwargs
                    )
                    decorator(action.target)
                else:
                    self._actions[action.name] = action
            else:
                assert callable(action)
                decorator = self.action(*args, **kwargs)
                decorator(action)
        else:
            raise ApplicationError(
                message=f'action already registered: {action.name}',
                data={'action': action}
            )

    def transaction(self):
        return self.local.tx_manager

    def commit(self):
        self.local.tx_manager.commit()

    def rollback(self):
        self.local.tx_manager.rollback()

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