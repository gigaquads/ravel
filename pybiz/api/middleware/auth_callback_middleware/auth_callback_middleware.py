from typing import Dict, Tuple

from pybiz.exc import NotAuthorizedError
from pybiz.util import is_sequence

from ..base import RegistryMiddleware
from .auth_callback import AuthCallback


class AuthCallbackMiddleware(RegistryMiddleware):
    """
    Apply the AuthCallback(s) associated with a proxy, set via the `auth`
    RegistryDecorator keyword argument, e.g., repl(auth=IsFoo()).
    NotAuthorizedError is raised if not authorized.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def on_request(self, proxy: 'RegistryProxy', args: Tuple, kwargs: Dict):
        """
        In on_request, args and kwargs are in the form output by
        registry.on_request.
        """
        callbacks = proxy.auth
        if not callbacks:
            return
        arguments = self._compute_arguments_dict(proxy, args, kwargs)
        context = dict()

        # ensure callbacks is a sequence for following for-loop
        if not is_sequence(callbacks):
            callbacks = [callbacks]
        # execute each AuthCallback, raising
        # NotAuthorizedError as soon as possible
        for authorize in callbacks:
            is_authorized = authorize(context, arguments)
            if not is_authorized:
                raise NotAuthorizedError()

    def _compute_arguments_dict(self, proxy, args, kwargs) -> Dict:
        """
        Merge all args and kwargs into a single Dict.
        """
        arguments = dict(
            zip([k for k in proxy.signature.parameters][:len(args)], args)
        )
        arguments.update(kwargs)
        return arguments
