import inspect
import traceback
import logging

from inspect import Signature
from typing import Dict, Text, Tuple, Callable

from appyratus.utils import TimeUtils

from ravel.util.loggers import console
from ravel.util.misc_functions import get_class_name, get_callable_name

from ravel.exceptions import RavelError


class EndpointError(RavelError):
    """
    An `Error` simply keeps a record of an Exception that occurs and the
    middleware object which raised it, provided there was one. This is used
    in the processing of requests, where the existence of errors has
    implications on control flow for middleware.
    """

    def __init__(self, exc, middleware=None):
        self.middleware = middleware
        self.timestamp = TimeUtils.utc_now()
        self.exc = exc
        if isinstance(exc, RavelError) and exc.wrapped_traceback:
            self.trace = self.exc.wrapped_traceback.strip().split('\n')[1:]
            self.exc_message = self.trace.pop().split(': ', 1)[1]
        else:
            self.trace = traceback.format_exc().strip().split('\n')[1:]
            self.exc_message = self.trace.pop().split(': ', 1)[1]

    def to_dict(self):
        data = {
            'timestamp': self.timestamp.isoformat(),
            'message': self.exc_message,
            'traceback': self.trace,
        }

        if isinstance(self.exc, RavelError):
            if not self.exc.wrapped_exception:
                data['exception'] = get_class_name(self.exc)
            else:
                data['exception'] = get_class_name(self.exc.wrapped_exception)

            if self.exc.logged_traceback_depth is not None:
                depth = self.exc.logged_traceback_depth
                data['traceback'] = data['traceback'][-2*depth:]

        if self.middleware is not None:
            data['middleware'] = get_class_name(self.middleware)

        if isinstance(self.exc, RavelError):
            data.update(self.exc.data)

        return data


class BadRequest(RavelError):
    """
    If any exceptions are raised during the execution of Middleware or an
    endpoint callable itself, we raise BadRequest.
    """

    def __init__(self,
        endpoint: 'Endpoint',
        state: 'ExecutionState',
        *args,
        **kwargs
    ):
        super().__init__(
            f'error/s occured in endpoint '
            f'"{endpoint.name}" (see logs)'
        )

        self.endpoint = endpoint
        self.state = state


class ExecutionState(object):
    def __init__(self, raw_args, raw_kwargs):
        self.errors = []
        self.target_error = None
        self.middleware = []
        self.raw_args = raw_args
        self.raw_kwargs = raw_kwargs
        self.processed_args = None
        self.processed_kwargs = None
        self.raw_result = None
        self.result = None


class Endpoint(object):

    Error = EndpointError
    State = ExecutionState
    BadRequest = BadRequest

    def __init__(self, func: Callable, decorator: 'EndpointDecorator'):
        self._is_bootstrapped = False
        self._decorator = decorator
        self._target = func.target if isinstance(func, Endpoint) else func
        self._signature = inspect.signature(self._target)
        self._api_object = decorator.api_object

    def __repr__(self):
        return f'{get_class_name(self)}(name={self.name})'

    def __getattr__(self, key: Text):
        return self.decorator.kwargs.get(key)

    def __call__(self, *raw_args, **raw_kwargs):
        state = Endpoint.State(raw_args, raw_kwargs)

        for func in (
            self._apply_middleware_pre_request,
            self._apply_app_on_request,
            self._apply_middleware_on_request,
            self._apply_target_callable,
            self._apply_app_on_response,
        ):
            error = func(state)
            if error is not None:
                break

        if state.target_error is None:
            self._apply_middleware_post_request(state)
        else:
            self._apply_middleware_post_bad_request(state)

        if state.errors:
            console.error(
                message=f'error/s occured in {self}',
                data={
                    'errors': [
                        error.to_dict() for error in state.errors
                    ]
                }
            )
            raise BadRequest(self, state)

        return state.result

    @property
    def target(self):
        return self._target

    @property
    def decorator(self):
        return self._decorator

    @property
    def app(self) -> 'Application':
        return self._decorator.app

    @property
    def api_object(self) -> 'Api':
        # TODO: rename this thing
        return self._api_object

    @property
    def is_bootstrapped(self) -> bool:
        return self._is_bootstrapped

    @property
    def name(self) -> Text:
        return get_callable_name(self._target)

    @property
    def docstring(self) -> Text:
        return inspect.getdoc(self._target)

    @property
    def signature(self) -> Signature:
        return self._signature

    @property
    def source(self) -> Text:
        return inspect.getsource(self.target)

    def bootstrap(self):
        self.on_bootstrap()
        self._is_bootstrapped = True

    def on_bootstrap(self):
        pass

    def on_call(self, args, kwargs):
        return self._target(*args, **kwargs)

    def _apply_middleware_pre_request(self, state):
        error = None
        for mware in self.app.middleware:
            if isinstance(self.app, mware.app_types):
                try:
                    mware.pre_request(self, state.raw_args, state.raw_kwargs)
                    state.middleware.append(mware)
                except Exception as exc:
                    error = EndpointError(exc, mware)
                    state.errors.append(error)
                    break
        return error

    def _apply_middleware_on_request(self, state):
        error = None
        for mware in state.middleware:
            try:
                mware.on_request(
                    self,
                    state.raw_args, state.raw_kwargs,
                    state.processed_args, state.processed_kwargs,
                )
            except Exception as exc:
                error = Endpoint.Error(exc, mware)
                state.errors.append(error)
        return error

    def _apply_middleware_post_request(self, state):
        error = None
        for mware in state.middleware:
            try:
                mware.post_request(
                    self,
                    state.raw_args, state.raw_kwargs,
                    state.processed_args, state.processed_kwargs,
                    state.result
                )
            except Exception as exc:
                error = Endpoint.Error(exc, mware)
                state.errors.append(error)
        return error

    def _apply_middleware_post_bad_request(self, state):
        error = None
        for mware in state.middleware:
            try:
                mware.post_bad_request(
                    self,
                    state.raw_args, state.raw_kwargs,
                    state.processed_args, state.processed_kwargs,
                    state.target_error.exc
                )
            except Exception as exc:
                error = Endpoint.Error(exc, mware)
                state.errors.append(error)
        return error

    def _apply_target_callable(self, state):
        try:
            error = None
            state.raw_result = self.on_call(
                state.processed_args,
                state.processed_kwargs
            )
        except Exception as exc:
            error = Endpoint.Error(exc)
            state.target_error = error
            state.errors.append(error)

        return error

    def _apply_app_on_response(self, state):
        error = None
        try:
            state.result = self.decorator.app.on_response(
                self,
                state.raw_result,
                state.raw_args,
                state.raw_kwargs,
                *state.processed_args,
                **state.processed_kwargs
            )
        except Exception as exc:
            error = Endpoint.Error(exc)
            state.errors.append(error)

        return error

    def _apply_app_on_request(self, state):
        error = None
        try:
            if self._api_object is not None:
                params = self.app.on_request(
                    self, self._api_object, *state.raw_args, **state.raw_kwargs
                )
            else:
                params = self.app.on_request(
                    self, *state.raw_args, **state.raw_kwargs
                )
            args, kwargs = (
                params if params
                else (state.raw_args, state.raw_kwargs)
            )
            state.processed_args, state.processed_kwargs = (
                self.app.loader.load(self, args, kwargs)
            )
        except Exception as exc:
            error = Endpoint.Error(exc)
            state.errors.append(error)

        return error


class AsyncEndpoint(Endpoint):
    """
    This specialized `Endpoint` can be used by any new `Application` type
    whose wrapped functions are coroutines.
    """
    # TODO: Implement
