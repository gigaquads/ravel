import inspect
import traceback
import logging

from typing import Dict, Text, Tuple

from pybiz.util.loggers import console

from ..exc import ProxyError, PybizError


class ApiProxy(object):
    """
    When a function is decorated with a `Api` object, the result is a
    `ApiProxy`. Its job is to transform the inputs received from an
    external client into the positional and keyword arguments expected by the
    wrapped function. Proxies also run middleware.
    """

    class Error(object):
        """
        An `Error` simply keeps a record of an Exception that occurs and the
        middleware object which raised it, provided there was one. This is used
        in the processing of requests, where the existence of errors has
        implications on control flow for middleware.
        """

        def __init__(self, exc, middleware=None):
            self.middleware = middleware
            self.trace = traceback.format_exc().split('\n')
            self.exc = exc

        def to_dict(self):
            return {
                'middleware': str(self.middleware),
                'exception': self.exc.__class__.__name__,
                'trace': self.trace,
            }

    def __init__(self, func, decorator: 'ApiDecorator'):
        """
        Args:
        - `func`: the callable being wrapped by this proxy (or another proxy).
        - `decorator`: the decorator that created this proxy.

        Attributes:
        - `target`: the native python function wrapped by this proxy.
        - `signature`: metadata on target, like its parameters, name, etc.
        """
        self.func = func
        self.decorator = decorator
        self.target = func.target if isinstance(func, ApiProxy) else func
        self.signature = inspect.signature(self.target)

    def __getattr__(self, key: Text):
        """
        Any keyword argument passed into the decorator which created this proxy
        are accessible via dot-notation on the proxy itself. For example,

        ```python
        @repl(foo='bar')
        def do_something():
            return 'something'

        assert repl.proxies['do_something'].foo == 'bar'
        ```
        """
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
        raw_args = list(raw_args)
        args, kwargs, error = self.pre_call(raw_args, raw_kwargs)
        raw_result = None
        if error is None:
            try:
                #  self.target is the wrapped function. we call it here!
                raw_result = self.target(*args, **kwargs)
            except Exception as exc:
                error = self.handle_target_exception(exc)
        result = self.post_call(
            raw_args, raw_kwargs, args, kwargs, raw_result, error
        )
        return result

    def __repr__(self):
        return f'<{self.__class__.__name__}({self.name})>'

    @property
    def api(self) -> 'Api':
        """
        Get the `Api` instance with which this proxy is registered.
        """
        return self.decorator.api

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

    def handle_target_exception(self, exc: Exception) -> Error:
        """
        This is where we go when an exception is raised in self.target when
        called in self.__call__.
        """
        error = ApiProxy.Error(exc)
        console.error(
            message=f'{self} failed',
            data=error.to_dict()
        )
        return error

    def pre_call(self, raw_args, raw_kwargs) -> Tuple[Tuple, Dict, Error]:
        """
        This is where we apply all logic in self.__call__ that precedes the
        actual calling of the wrapped "target" function. This is where we
        process raw inputs into expected args and kwargs.
        """
        # apply pre_request middleware, etc
        error = self.pre_request(raw_args, raw_kwargs)
        if error is None:
            # get prepared args and kwargs from Api.on_request,
            # followed by middleware on_request.
            args, kwargs, error = self.on_request(raw_args, raw_kwargs)
        else:
            args = tuple()
            kwargs = {}
        return (args, kwargs, error)

    def post_call(
        self, raw_args, raw_kwargs, args, kwargs, raw_result, error
    ) -> object:
        """
        Here we apply any clean up logic, finalization and data marshaling that
        may need to occur at the end on self.__call__, after the wrapped
        "target" function has been called.
        """
        result, errors = self.post_request(
            raw_args, raw_kwargs, args, kwargs, raw_result, error
        )
        # if any exceptions were raised in the processing of the request
        # we raise a higher-level exception that contains a list of all errors,
        # which in turn contain a stack trace and Exception object.
        if errors:
            console.error(data=[
                err.to_dict() for err in errors
            ])
            raise ProxyError(errors)
        return result

    def pre_request(self, raw_args, raw_kwargs):
        """
        Apply middleware pre_request methods, which take raw args and kwargs
        received from the client.
        """
        # middleware pre-request logic
        try:
            for mware in self.api.middleware:
                if isinstance(self.api, mware.api_types):
                    mware.pre_request(self, raw_args, raw_kwargs)
        except Exception as exc:
            error = ApiProxy.Error(exc, mware)
            console.error(
                message=f'{mware.__class__.__name__}.pre_request failed',
                data=error.to_dict()
            )
            return error
        return None

    def on_request(self, raw_args, raw_kwargs) -> Tuple[Tuple, Dict, Error]:
        """
        Transforms raw positional and keyword arguments according to what the
        wrapped (target) function expects and then executes on_request
        middleware methods.
        """
        # get args and kwargs from native inputs
        try:
            params = self.api.on_request(self, *raw_args, **raw_kwargs)
            args, kwargs = params if params else (raw_args, raw_kwargs)
        except Exception as exc:
            return (tuple(), {}, ApiProxy.Error(exc))

        # load BizObjects from ID's passed into the proxy in place
        args, kwargs = self.api.argument_loader.load(self, args, kwargs)
        args = args if isinstance(args, list) else list(args)

        # middleware on_request logic
        try:
            for mware in self.api.middleware:
                if isinstance(self.api, mware.api_types):
                    mware.on_request(self, args, kwargs)
            return (args, kwargs, None)
        except Exception as exc:
            error = ApiProxy.Error(exc, mware)
            console.error(
                message=f'{mware.__class__.__name__}.on_request failed',
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
            result = self.decorator.api.on_response(
                self, raw_result, raw_args, raw_kwargs, *args, **kwargs
            )
        except Exception as exc:
            result = None
            errors.append(ApiProxy.Error(exc))
            console.error(
                message=f'{self.api}.on_response failed',
                data=errors[-1].to_dict()
            )

        # run middleware post-request logic
        for mware in self.api.middleware:
            if isinstance(self.api, mware.api_types):
                try:
                    mware.post_request(
                        self, raw_args, raw_kwargs, args, kwargs, result
                    )
                except Exception as exc:
                    errors.append(PybizError(exc, mware))
                    console.error(
                        message=(
                            f'{mware.__class__.__name__}.post_request failed'
                        ),
                        data=errors[-1].to_dict()
                    )
            if (error is not None) and (mware is error.middleware):
                # only process middleware up to the point where middleware
                # failed in either pre_request or on_request.
                break

        return (result, errors)


class AsyncProxy(ApiProxy):
    """
    This specialized `ApiProxy` can be used by any new `Api` type
    whose wrapped functions are coroutines.
    """

    async def __call__(self, *raw_args, **raw_kwargs):
        args, kwargs, error = self.pre_call(raw_args, raw_kwargs)
        raw_result = None
        if error is None:
            try:
                # self.target is the wrapped function. we call it here!
                raw_result = await self.target(*args, **kwargs)
            except Exception as exc:
                error = self.handle_target_exception(exc)
        result = self.post_call(
            raw_args, raw_kwargs, args, kwargs, raw_result, error
        )
        return result

    def __repr__(self):
        return f'<{self.__class__.__name__}(async {self.name})>'
