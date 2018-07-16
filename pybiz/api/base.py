import os
import inspect
import importlib
import traceback

import venusian
import yaml

from abc import ABCMeta, abstractmethod
from collections import defaultdict
from appyratus.validation import Schema, fields

from pybiz.dao.base import Dao
from pybiz.manifest import Manifest
from pybiz.exc import ApiError


class FunctionRegistry(object, metaclass=ABCMeta):
    def __init__(self):
        self._bootstrapped = False
        self._manifest = None
        self._decorators = []

    def __call__(
        self,
        hook=None,    # TODO: rename to callback
        unpack=None,  # TODO: rename to bind_arguments... or something
        *args,
        **kwargs
    ):
        """
        Use this to decorate functions, adding them to this FunctionRegistry.
        Each time a function is decorated, it arives at the abstract "hook"
        method, where you can registry the function with a web framework or
        whatever system you have in mind.

        Usage:

        ```python3
            api = FunctionRegistry()

            @api()
            def do_something():
                pass
        ```
        """
        decorator = self.function_decorator_type(
            self,
            hook=hook or self.hook,
            unpack=unpack or self.unpack,
            *args, **kwargs
        )
        self._decorators.append(decorator)
        return decorator

    @property
    def function_decorator_type(self):
        return FunctionDecorator

    @property
    def function_proxy_type(self):
        return FunctionProxy

    @property
    def manifest(self):
        return self._manifest

    @property
    def bootstrapped(self):
        return self._bootstrapped

    def bootstrap(self, filepath: str=None):
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
    def start(self):
        """
        Enter the main loop in whatever program context your FunctionRegistry is
        being used, like in a web framework or a REPL.
        """

    @abstractmethod
    def hook(self, proxy: 'FunctionProxy'):
        """
        We come here whenever a function is decorated by this registry. Here we
        can add the decorated function to, say, a web framework as a route.
        """

    def pack(self, result, *args, **kwargs):
        """
        Defines how the return value from API callables is set on the HTTP
        response used by whatever web framework you're using.

        Take the Falcon web framework for example. Normally, Falcon does not
        expect return values from its request proxies. Instead, it does
        `response.body = result`. By defining unpack, we can have it our way by
        defining custom logic to apply to the return value of our proxies. For
        example:

        ```python3
        def pack(data, *args, **kwargs):
            # *args is whatever Falcon passed into the proxy.
            response = args[1]
            response.body = data
        ```
        """
        return result

    def unpack(self, signature, *args, **kwargs):
        """
        Defines how the arguments passed into API proxies from your web
        framework are transformed into the expected arguments.

        For example, Falcon would require our proxies to have the following
        function signature:

        ```python3
            def login(request, response):
                pass
        ```

        By implementing `unpack`, we could extract the top-level fields in the
        request JSON payload, say "email" and "password", and pass them into the
        proxy directly, like so:

        ```python3
            def login(email, password):
                pass
        ```
        """
        return (args, kwargs)


class FunctionDecorator(object):
    def __init__(self,
        registry,
        hook=None,
        unpack=None,
        **params
    ):
        self.registry = registry
        self.hook = hook
        self.unpack = unpack
        self.params = params

    def __call__(self, func):
        proxy = self.registry.function_proxy_type(func, self)
        if self.hook is not None:
            self.hook(proxy)
        return proxy


class FunctionProxy(object):
    def __init__(self, func, decorator):
        self.func = func
        self.signature = inspect.signature(self.func)
        self.decorator = decorator

    def __repr__(self):
        return '<FunctionProxy({})>'.format(', '.join([
                'method={}'.format(self.func.__name__)
            ]))

    def __call__(self, *args, **kwargs):
        try:
            unpack = self.decorator.unpack
            unpacked = unpack(self.signature, *args, **kwargs)
            unpacked_args, unpacked_kwargs = unpacked
        except KeyError as exc:
            raise ApiError(
                'Could not unpack request arguments. Missing '
                '{} argument.'.format(str(exc)))
        except Exception:
            msg = traceback.format_exc()
            raise ApiError(
                '{} - Could not unpack request arguments.'.format(msg))

        result = self.func(*unpacked_args, **unpacked_kwargs)
        self.decorator.registry.pack(result, *args, **kwargs)
        return result

    @property
    def func_name(self):
        return self.func.__name__ if self.func else None
