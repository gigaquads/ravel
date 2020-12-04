from typing import Dict, Tuple

from appyratus.utils import DictObject

from ravel.exceptions import NotAuthorized
from ravel.util.misc_functions import is_sequence, normalize_to_tuple
from ravel.app.middleware.middleware import Middleware

from .guard import Guard, GuardFailure


class EvaluateGuards(Middleware):
    """
    Apply the Guard(s) associated with a action, set via the `auth`
    ActionDecorator keyword argument, e.g., repl(auth=IsFoo()).
    NotAuthorized is raised if not authorized.
    """

    def on_request(
        self,
        action: 'Action',
        request: 'Request',
        processed_args: Tuple,
        processed_kwargs: Dict,
    ):
        """
        If the requested action has a guard, we execute it here. If it fails,
        we raise a GuardFailure exception.
        """
        guard = self._resolve_guard(action)
        if guard is not None:
            # merge all positional and keyword args into a single dict
            arguments = self._extract_args(
                action, processed_args, processed_kwargs
            )
            # execute the guard (and composite guards, recursively)
            guard_passed = guard(request, arguments=arguments)
            if guard_passed is False:
                raise GuardFailure(guard, "guard failed")

    def _resolve_guard(self, action):
        """
        Return the guard associated with the action if it exists. The argument
        is expected to be a Guard instance or a callback function that returns
        one.
        """
        guard = action.guard
        if not guard:
            return None
        elif not isinstance(guard, Guard) and callable(guard):
            guard = guard()
        return guard

    def _extract_args(self, action, args, kwargs) -> Dict:
        """
        Merge all args and kwargs into a single Dict. This is is passed into
        each executed Guard as **kwargs.
        """
        arg_names = [k for k in action.signature.parameters][:len(args)]
        merged = dict(zip(arg_names, args))
        merged.update(kwargs)
        return merged
