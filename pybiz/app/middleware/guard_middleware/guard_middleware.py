from typing import Dict, Tuple

from pybiz.exceptions import NotAuthorized
from pybiz.util.misc_functions import is_sequence, normalize_to_tuple
from ..base import Middleware
from .guard import Guard, GuardFailure


class GuardMiddleware(Middleware):
    """
    Apply the Guard(s) associated with a endpoint, set via the `auth`
    EndpointDecorator keyword argument, e.g., repl(auth=IsFoo()).
    NotAuthorized is raised if not authorized.
    """

    def on_request(
        self,
        endpoint: 'Endpoint',
        raw_args: Tuple,
        raw_kwargs: Dict,
        processed_args: Tuple,
        processed_kwargs: Dict,
    ):
        """
        If the requested endpoint has a guard, we execute it here. If it fails,
        we raise a GuardFailure exception.
        """
        guard = self._extract_guard(endpoint)
        if guard is not None:
            #
            arguments = self._extract_args(
                endpoint, processed_args, processed_kwargs
            )
            # recursively execute the guard
            guard_passed = guard(context={}, arguments=arguments)
            if not guard_passed:
                raise GuardFailure(guard, "guard failure")

    def _extract_guard(self, endpoint):
        guard = endpoint.guard
        if not guard:
            return None
        elif not isinstance(guard, Guard) and callable(guard):
            guard = guard()
        return guard

    def _extract_args(self, endpoint, args, kwargs) -> Dict:
        """
        Merge all args and kwargs into a single Dict.
        """
        arg_names = [k for k in endpoint.signature.parameters][:len(args)]
        merged = dict(zip(arg_names, args))
        merged.update(kwargs)
        return merged
