import os
import inspect
import importlib
import traceback
import venusian
import yaml
from abc import ABCMeta, abstractmethod
from collections import defaultdict

from pybiz.dao.base import DaoManager
from pybiz.manifest import Manifest
from appyratus.validation import Schema, fields

from .exc import ApiError
from .const import (
    HTTP_GET,
    HTTP_POST,
    HTTP_PUT,
    HTTP_PATCH,
    HTTP_DELETE,
)


class ApiRegistry(object, metaclass=ABCMeta):

    _instance = None

    @classmethod
    def get_instance(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = cls(*args, **kwargs)
        return cls._instance

    def __init__(self):
        self._handlers = defaultdict(dict)
        self._bootstrapped = False
        self._manifest = None

    @property
    def handlers(self):
        return self._handlers

    @property
    def manifest(self):
        return self._manifest

    @property
    def bootstrapped(self):
        return self._bootstrapped

    def bootstrap(self, filepath: str = None):
        """
        Bootstrap the data, business, and service layers, wiring them up,
        according to the settings contained in a service manifest file.

        Args:
            - filepath: Path to manifest.yml file
        """
        if not self._bootstrapped:
            self._bootstrapped = True
            if self._manifest is None or filepath is not None:
                self._manifest = Manifest(self, filepath=filepath)
            if self._manifest is not None:
                self._manifest.process()

    @abstractmethod
    def hook(self, handler):
        """
        This `hook` method is called upon each use of an HTTP decorator method
        (defined below). This is where you can register each handler with your
        native web framework.
        """

    @abstractmethod
    def pack(self, result, *args, **kwargs):
        """
        Defines how the return value from API callables is set on the HTTP
        response used by whatever web framework you're using.

        Take the Falcon web framework for example. Normally, Falcon does not
        expect return values from its request handlers. Instead, it does
        `response.body = result`. By defining unpack, we can have it our way by
        defining custom logic to apply to the return value of our handlers. For
        example:

        ```python3
        def pack(data, *args, **kwargs):
            # *args is whatever Falcon passed into the handler.
            response = args[1]
            response.body = data
        ```
        """

    @abstractmethod
    def unpack(self, signature, *args, **kwargs):
        """
        Defines how the arguments passed into API handlers from your web
        framework are transformed into the expected arguments.

        For example, Falcon would require our handlers to have the following
        function signature:

        ```python3
            def login(request, response):
                pass
        ```

        By implementing `unpack`, we could extract the top-level fields in the
        request JSON payload, say "email" and "password", and pass them into the
        handler directly, like so:

        ```python3
            def login(email, password):
                pass
        ```
        """

    def get(self, path, schemas=None, hook=None, unpack=None):
        hook = hook or self.hook
        unpack = unpack or self.unpack
        return ApiRegistryDecorator(
            self, HTTP_GET, path, schemas=schemas, hook=hook, unpack=unpack
        )

    def post(self, path, schemas=None, hook=None, unpack=None):
        hook = hook or self.hook
        unpack = unpack or self.unpack
        return ApiRegistryDecorator(
            self, HTTP_POST, path, schemas=schemas, hook=hook, unpack=unpack
        )

    def put(self, path, schemas=None, hook=None, unpack=None):
        hook = hook or self.hook
        unpack = unpack or self.unpack
        return ApiRegistryDecorator(
            self, HTTP_PUT, path, schemas=schemas, hook=hook, unpack=unpack
        )

    def patch(self, path, schemas=None, hook=None, unpack=None):
        hook = hook or self.hook
        unpack = unpack or self.unpack
        return ApiRegistryDecorator(
            self, HTTP_PATCH, path, schemas=schemas, hook=hook, unpack=unpack
        )

    def delete(self, path, schemas=None, hook=None, unpack=None):
        hook = hook or self.hook
        unpack = unpack or self.unpack
        return ApiRegistryDecorator(
            self, HTTP_DELETE, path, schemas=schemas, hook=hook, unpack=unpack
        )

    def route(self, http_method, path, handler_args=None, handler_kwargs=None):
        handler = self.handlers[path.lower()][http_method.lower()]
        handler_args = handler_args or tuple()
        handler_kwargs = handler_kwargs or dict()
        return handler(*handler_args, **handler_kwargs)


class ApiRegistryDecorator(object):
    def __init__(
        self,
        registry,
        http_method: str,
        path: str,
        schemas: dict = None,
        hook=None,
        unpack=None,
    ):

        self.registry = registry
        self.http_method = http_method.lower()
        self.path = path.lower()
        self.schemas = schemas or {}
        self.hook = hook
        self.unpack = unpack

    def __call__(self, func):
        handler = ApiHandler(func, self)
        if self.hook is not None:
            self.hook(handler)
        self.registry.handlers[self.path][self.http_method] = handler
        return handler


class ApiHandler(object):
    def __init__(self, target, decorator):
        self.target = target
        self.signature = inspect.signature(self.target)
        self.decorator = decorator

    def __repr__(self):
        return '<ApiHandler({})>'.format(
            ', '.join(
                [
                    'method={}'.format(self.decorator.http_method.upper()),
                    'path={}'.format(self.decorator.path),
                ]
            )
        )

    def __call__(self, *args, **kwargs):
        try:
            unpack = self.decorator.unpack
            target_args_dict = unpack(self.signature, *args, **kwargs)
        except KeyError as exc:
            raise ApiError(
                'Could not unpack request arguments. Missing '
                '{} argument.'.format(str(exc))
            )
        except Exception:
            msg = traceback.format_exc()
            raise ApiError(
                '{} - Could not unpack request arguments.'.format(msg)
            )

        result = self.target(**target_args_dict)
        self.decorator.registry.pack(result, *args, **kwargs)
        return result

    @property
    def http_method(self):
        return self.decorator.http_method

    @property
    def path(self):
        return self.decorator.path

    @property
    def schemas(self):
        return self.decorator.schemas
