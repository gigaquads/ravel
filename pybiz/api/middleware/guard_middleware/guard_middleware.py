from typing import Dict, Tuple

from pybiz.exc import NotAuthorizedError
from pybiz.util import is_sequence

from ..base import RegistryMiddleware
from .guard import Guard, GuardFailed


class GuardMiddleware(RegistryMiddleware):
    """
    Apply the Guard(s) associated with a proxy, set via the `auth`
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
        guards = proxy.guards
        if not guards:
            return
        arguments = self._compute_arguments_dict(proxy, args, kwargs)
        context = dict()

        # ensure guards is a sequence for following for-loop
        if not is_sequence(guards):
            guards = [guards]
        # execute each Guard, raising
        # NotAuthorizedError as soon as possible
        for guard in guards:
            ok = guard(context, arguments)
            if not ok:
                raise GuardFailed(guard)

    def _compute_arguments_dict(self, proxy, args, kwargs) -> Dict:
        """
        Merge all args and kwargs into a single Dict.
        """
        arguments = dict(
            zip([k for k in proxy.signature.parameters][:len(args)], args)
        )
        arguments.update(kwargs)
        return arguments
