import inspect
import traceback
import logging

from inspect import Signature
from typing import Dict, Text, Tuple, Callable
from collections import deque

from appyratus.utils.time_utils import TimeUtils
from appyratus.utils.dict_utils import DictObject

from ravel.util.loggers import console
from ravel.util.misc_functions import get_class_name, get_callable_name

from ravel.exceptions import RavelError


class ActionError(RavelError):
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
            self.exc_message = self.trace[-1].split(': ', 1)[1]
        else:
            self.trace = traceback.format_exc().strip().split('\n')[1:]
            final_line = self.trace[-1]
            if ':' in final_line:
                self.exc_message = final_line.split(':', 1)[1]
            else:
                self.exc_message = None

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
    action callable itself, we raise BadRequest.
    """

    def __init__(self,
        action: 'Action',
        state: 'ExecutionState',
        *args,
        **kwargs
    ):
        super().__init__(
            f'error/s occured in action '
            f'"{action.name}" (see logs)'
        )

        self.action = action
        self.state = state


class ExecutionState(object):
    def __init__(self, action, raw_args, raw_kwargs):
        self.action = action
        self.errors = []
        self.exc = None
        self.middleware = []
        self.raw_args = raw_args
        self.raw_kwargs = raw_kwargs
        self.processed_args = None
        self.processed_kwargs = None
        self.raw_result = None
        self.result = None
        self.is_complete = False

    @property
    def app(self) -> 'Application':
        return self.action.app


class Request(object):
    def __init__(self, state):
        self.__dict__['internal'] = state
        self.__dict__['context'] = DictObject()

    def __repr__(self):
        return f'Request(action="{self.internal.action.name}")'

    def __getattr__(self, key):
        return self.context.get(key)

    @property
    def is_complete(self) -> bool:
        return self.internal.is_complete

    @is_complete.setter
    def is_complete(self, is_complete: bool):
        self.internal.is_complete = is_complete

    @property
    def raw_args(self):
        return self.internal.raw_args

    @property
    def raw_kwargs(self):
        return self.internal.raw_kwargs

    @property
    def session(self) -> 'Session':
        return self.context.session

    @session.setter
    def session(self, session: 'Session'):
        self.context.session = session

    @property
    def app(self) -> 'Application':
        return self.internal.app


class Action(object):

    Error = ActionError
    State = ExecutionState
    BadRequest = BadRequest

    def __init__(self, func: Callable, decorator: 'ActionDecorator'):
        self._is_bootstrapped = False
        self._decorator = decorator
        self._target = func.target if isinstance(func, Action) else func
        self._signature = inspect.signature(self._target)
        self._api_object = decorator.api_object

    def __repr__(self):
        return f'{get_class_name(self)}(name={self.name})'

    def __getattr__(self, key: Text):
        return self.decorator.kwargs.get(key)

    def __call__(self, *raw_args, **raw_kwargs):
        state = ExecutionState(self, raw_args, raw_kwargs)
        request = Request(state)

        for func in (
            self._apply_middleware_pre_request,
            self._apply_app_on_request,
            self._apply_middleware_on_request,
            self._apply_target_callable,
            self._apply_app_on_response,
        ):
            error = func(request)
            if (error is not None) or request.is_complete:
                break

        if state.exc is None:
            self._apply_middleware_post_request(request)
        else:
            self._apply_middleware_post_bad_request(request)

        if state.errors:
            console.error(
                message=f'error/s occured in action: {self.name}',
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
        console.debug(f'bootstrapping {self.name} action')
        self._prepare_middleware()
        self.on_bootstrap()
        self._is_bootstrapped = True

    def on_bootstrap(self):
        pass

    def _prepare_middleware(self):
        """
        Determine which middleware hook methods will or will potentially be
        invoked during the execution of this action. This is computed on
        bootstrap.
        """
        self._mware_pre_request_methods = deque()
        self._mware_on_request_methods = deque()
        self._mware_post_request_methods = deque()
        self._mware_post_bad_request_methods = deque()

        def is_virtual(func):
            return getattr(func, 'is_virtual', False)

        for mware in self.app.middleware:
            if not isinstance(self.app, mware.app_types):
                continue

            if not is_virtual(mware.pre_request):
                self._mware_pre_request_methods.append(mware.pre_request)
            if not is_virtual(mware.on_request):
                self._mware_on_request_methods.append(mware.on_request)
            if not is_virtual(mware.post_request):
                self._mware_post_request_methods.appendleft(mware.post_request)
            if not is_virtual(mware.post_bad_request):
                self._mware_post_bad_request_methods.appendleft(mware.post_bad_request)

    def _apply_middleware_pre_request(self, request):
        state = request.internal
        error = None

        for func in self._mware_pre_request_methods:
            try:
                func(self, request, state.raw_args, state.raw_kwargs)
                state.middleware.append(func)
            except Exception as exc:
                error = ActionError(exc, func)
                state.errors.append(error)
                break

            # if is_complete, we abort further mware processing as well as all
            # subsequent steps in processing the action. instead, we skip
            # directly to post_request middleware execution.
            if request.is_complete:
                break

        return error

    def _apply_middleware_on_request(self, request):
        state = request.internal
        error = None

        for func in self._mware_on_request_methods:
            args = state.processed_args
            kwargs = state.processed_kwargs

            try:
                func(self, request, args, kwargs)
            except Exception as exc:
                error = Action.Error(exc, mware)
                state.errors.append(error)

        return error

    def _apply_middleware_post_request(self, request):
        state = request.internal
        error = None

        for func in self._mware_post_request_methods:
            try:
                func(self, request, state.result)
            except Exception as exc:
                error = Action.Error(exc, func)
                state.errors.append(error)

        return error

    def _apply_middleware_post_bad_request(self, request):
        state = request.internal
        error = None

        for func in self._mware_post_bad_request_methods:
            try:
                func(self, request, state.exc.exc)
            except Exception as exc:
                error = Action.Error(exc, mware)
                state.errors.append(error)

        return error

    def _apply_target_callable(self, request):
        state = request.internal
        try:
            error = None
            takes_arguments = len(self.signature.parameters) > 1
            if takes_arguments:
                state.raw_result = self._target(
                    request,
                    *state.processed_args,
                    **state.processed_kwargs
                )
            else:
                state.raw_result = self._target(request)
        except Exception as exc:
            error = Action.Error(exc)
            state.exc = error
            state.errors.append(error)

        return error

    def _apply_app_on_response(self, request):
        state = request.internal
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
            error = Action.Error(exc)
            state.errors.append(error)

        return error

    def _apply_app_on_request(self, request):
        state = request.internal
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
                self.marshal(args, kwargs)
            )
        except Exception as exc:
            error = Action.Error(exc)
            state.errors.append(error)

        return error

    def marshal(self, raw_args, raw_kwargs):
        return self.app.loader.load(self, raw_args, raw_kwargs)


class AsyncAction(Action):
    """
    This specialized `Action` can be used by any new `Application` type
    whose wrapped functions are coroutines.
    """
    # TODO: Implement
