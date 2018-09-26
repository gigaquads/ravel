import os
import inspect
import importlib
import traceback

import venusian
import yaml

from collections import defaultdict
from typing import Dict, Text
from threading import local

from appyratus.validation import Schema, fields
from appyratus.decorators import memoized_property
from appyratus.types import DictAccessor

from pybiz.dao.base import Dao
from pybiz.manifest import Manifest
from pybiz.exc import ApiError


class FunctionRegistry(object):
    def __init__(self, manifest=None):
        self.thread_local = local()
        self.decorators = []
        self.proxies = []
        self._manifest = manifest
        self._is_bootstrapped = False

    def __call__(self, *args, **kwargs):
        """
        Use this to decorate functions, adding them to this FunctionRegistry.
        Each time a function is decorated, it arives at the "on_decorate"
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
        decorator = self.function_decorator_type(self, *args, **kwargs)
        self.decorators.append(decorator)
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
        """
        if not self.is_bootstrapped:
            if (self._manifest is None) or (filepath is not None):
                self._manifest = Manifest(manifest_filepath)
            if (self.manifest is not None) and (not defer_processing):
                self._manifest.process()
            self._is_bootstrapped = True

    def start(self, *args, **kwargs):
        """
        Enter the main loop in whatever program context your FunctionRegistry is
        being used, like in a web framework or a REPL.
        """
        raise NotImplementedError('override in subclass')

    def on_decorate(self, proxy: 'FunctionProxy'):
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


class FunctionDecorator(object):
    def __init__(self, registry, *args, **params):
        self.registry = registry
        self.params = params

    def __call__(self, func):
        proxy = self.registry.function_proxy_type(func, self)
        self.registry.proxies.append(proxy)
        self.registry.on_decorate(proxy)
        return proxy


class FunctionProxy(object):
    def __init__(self, func, decorator):
        self.func = func
        self.signature = inspect.signature(self.func)
        self.target = self.resolve(func)
        self.decorator = decorator
        self.on_request = decorator.registry.on_request
        self.on_response = decorator.registry.on_response

    def __repr__(self):
        return '<{}({})>'.format(
            self.__class__.__name__,
            ', '.join(['method={}'.format(self.func.__name__)])
        )

    def __call__(self, *raw_args, **raw_kwargs):
        on_request = self.decorator.registry.on_request
        on_request_retval = on_request(
            self, self.signature, *raw_args, **raw_kwargs
        )
        if on_request_retval:
            prepared_args, prepared_kwargs = on_request_retval
        else:
            prepared_args, prepared_kwargs = raw_args, raw_kwargs
        result = self.target(*prepared_args, **prepared_kwargs)
        processed_result = self.decorator.registry.on_response(
            self, result, *raw_args, **raw_kwargs
        )
        return processed_result or result

    def __getattr__(self, attr):
        return getattr(self.func, attr)

    @property
    def name(self):
        return self.target.__name__

    def resolve(self, func):
        return func.target if isinstance(func, FunctionProxy) else func

    def call_target(self, raw_args, raw_kwargs, pybiz_debug=False):
        sig = self.signature
        arguments = self.on_request(self, sig, *raw_args, **raw_kwargs)
        args, kwargs = arguments if arguments else (raw_args, raw_kwargs)
        if pybiz_debug:
            breakpoint()
        retval = self.target(*args, **kwargs)
        self.on_response(self, retval, *raw_args, **raw_kwargs)
        return retval
