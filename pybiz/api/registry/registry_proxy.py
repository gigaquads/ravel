import inspect
import traceback
import logging

from typing import Dict, Text

from pybiz.logging import ConsoleLoggerInterface
from pybiz.constants import CONSOLE_LOG_LEVEL

from ..exc import RegistryProxyError


class RegistryProxy(object):
    """
    When a function is decorated with a `Registry` object, the result is a
    `RegistryProxy`. Its job is to transform the inputs received from an
    external client into the positional and keyword arguments expected by the
    wrapped function. Proxies also run middleware.
    """

    log = ConsoleLoggerInterface(__name__, level=CONSOLE_LOG_LEVEL)

    class Error(object):
        """
        An `Error` simply keeps a record of an Exception that occurs and the
        middleware object which raised it, provided there was one. This is used
        in the processing of requests, where the existence of errors has
        implications on control flow for middleware.
        """
        def __init__(self, exc, middleware):
            self.middleware = middleware
            self.trace = traceback.format_exc().split('\n')
            self.exc = exc

        def to_dict(self):
            return {
                'middleware': str(self.middleware),
                'exception': self.exc.__class__.__name__,
                'trace': self.trace,
            }

    def __init__(self, func, decorator: 'RegistryDecorator'):
        self.func = func
        self.decorator = decorator
        self.target = func.target if isinstance(func, RegistryProxy) else func
        self.signature = inspect.signature(self.target)

    def __repr__(self):
        return f'<{self.__class__.__name__}({self.name})>'

    def __getattr__(self, key: Text):
        return self.decorator.kwargs.get(key)

    def __call__(self, *raw_args, **raw_kwargs):
        """
        If any middleware in pre_request, on_request, or during the invocation
        of the proxy target function, we do not raise the exception but pass it
        forward into post_request, so that we may still execute middleware
        post_request logic up to the middleware that failed earlier.

        During post_request, we aggregate any existing error with all errors
        generated by any middleware while running its post_request method. If
        any errors exist at this, point an exception is finally raised.
        """
        result = raw_result = kwargs = args = None

        # apply pre_request middleware, etc
        error = self.pre_request(raw_args, raw_kwargs)
        if error is None:
            # get prepared args and kwargs from Registry.on_request,
            # followed by middleware on_request.
            args, kwargs, error = self.on_request(raw_args, raw_kwargs)
            if error is None:
                # fire in the hole!
                try:
                    raw_result = self.target(*args, **kwargs)
                except Exception as exc:
                    error = RegistryProxy.Error(exc, None)
                    self.log.exception(
                        message='{self}.target failed',
                        data=error.to_dict()
                    )

        # perform teardown, running middleware post_requests.
        result, errors = self.post_request(
            raw_args, raw_kwargs, args, kwargs, raw_result, error
        )
        # if any exceptions were raised in the processing of the request
        # we raise a higher-level exception that contains a list of all errors,
        # which in turn contain a stack trace and Exception object.
        if errors:
            raise RegistryProxyError(errors)

        return result

    @property
    def registry(self) -> 'Registry':
        """
        Get the `Registry` instance with which this proxy is registered.
        """
        return self.decorator.registry

    @property
    def name(self) -> Text:
        """
        Return the name of the underlying function to which this proxy proxies.
        """
        return self.target.__name__

    @property
    def docstring(self) -> Text:
        """
        Return a formmated string of the target proxy function's source code.
        """
        return inspect.getdoc(self.target)

    def pre_request(self, raw_args, raw_kwargs):
        """
        Apply middleware pre_request methods, which take raw args and kwargs
        received from the client.
        """
        # middleware pre-request logic
        try:
            for mware in self.registry.middleware:
                if isinstance(self.registry, mware.registry_types):
                    mware.pre_request(self, raw_args, raw_kwargs)
        except Exception as exc:
            error = RegistryProxy.Error(exc, mware)
            self.log.exception(
                message='{self}.pre_request failed',
                data=error.to_dict()
            )
            return error
        return None

    def on_request(self, raw_args, raw_kwargs):
        """
        Transforms raw positional and keyword arguments according to what the
        wrapped (target) function expects and then executes on_request
        middleware methods.
        """
        # get args and kwargs from native inputs
        try:
            params = self.registry.on_request(self, *raw_args, **raw_kwargs)
            args, kwargs = params if params else (raw_args, raw_kwargs)
        except Exception as exc:
            return (tuple(), {}, RegistryProxy.Error(exc, None))

        # middleware on_request logic
        try:
            for mware in self.registry.middleware:
                if isinstance(self.registry, mware.registry_types):
                    mware.on_request(self, args, kwargs)
            return (args, kwargs, None)
        except Exception as exc:
            error = RegistryProxy.Error(exc, mware)
            self.log.exception(
                message='{self}.pre_request failed',
                data=error.to_dict()
            )
            return (args, kwargs, error)

    def post_request(
        self, raw_args, raw_kwargs, args, kwargs, raw_result, error
    ):
        """
        Transform the native output from the wrapped function into the data
        structure expected by the external client. Afterwards, run middleware
        post_request logic (up to the point in the middleware sequence where an
        error occurred, assuming one did).
        """
        errors = [] if not error else [error]

        # prepare the proxy "result" return value
        try:
            result = self.decorator.registry.on_response(
                self, raw_result, *args, **kwargs
            )
        except Exception as exc:
            result = None
            errors.append(RegistryProxy.Error(exc, None))
            self.log.exception(
                message=f'{self.registry}.on_response failed',
                data=errors[-1].to_dict()
            )

        # run middleware post-request logic
        for mware in self.registry.middleware:
            if isinstance(self.registry, mware.registry_types):
                try:
                    mware.post_request(
                        self,
                        raw_args, raw_kwargs,
                        args, kwargs,
                        result
                    )
                except Exception as exc:
                    errors.append(PybizError(exc, mware))
                    self.log.exception(
                        message=f'{mware}.post_response failed',
                        data=errors[-1].to_dict()
                    )
            if mware is error.middleware:
                # only process middleware up to the point where middleware
                # failed in either pre_request or on_request.
                break

        return (result, errors)


class AsyncRegistryProxy(RegistryProxy):
    """
    This specialized `RegistryProxy` can be used by any new `Registry` type
    whose wrapped functions are coroutines.
    """

    async def __call__(self, *raw_args, **raw_kwargs):
        result = raw_result = kwargs = args = None

        # apply pre_request middleware, etc
        error = self.pre_request(raw_args, raw_kwargs)
        if error is None:
            # get prepared args and kwargs from Registry.on_request,
            # followed by middleware on_request.
            args, kwargs, error = self.on_request(raw_args, raw_kwargs)
            if error is None:
                # fire in the hole!
                try:
                    raw_result = await self.target(*args, **kwargs)
                except Exception as exc:
                    error = RegistryProxy.Error(exc, None)

        # perform teardown, running middleware post_requests.
        result, errors = self.post_request(
            raw_args, raw_kwargs, args, kwargs, raw_result, error
        )
        # if any exceptions were raised in the processing of the request
        # we raise a higher-level exception that contains a list of all errors,
        # which in turn contain a stack trace and Exception object.
        if errors:
            raise RegistryProxyError(errors)

        return result

    def __repr__(self):
        return f'<{self.__class__.__name__}(async {self.name})>'
