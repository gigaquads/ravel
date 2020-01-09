from typing import Dict, Tuple

from pybiz.exceptions import NotAuthorized
from pybiz.util.misc_functions import is_sequence

from ..base import Middleware
from .guard import Guard, GuardFailed


class GuardMiddleware(Middleware):
    """
    Apply the Guard(s) associated with a endpoint, set via the `auth`
    EndpointDecorator keyword argument, e.g., repl(auth=IsFoo()).
    NotAuthorized is raised if not authorized.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def on_request(self, endpoint: 'Endpoint', args: Tuple, kwargs: Dict):
        """
        In on_request, args and kwargs are in the form output by app.on_request.
        """
        guards = endpoint.guards
        if not guards:
            return
        arguments = self._compute_arguments_dict(endpoint, args, kwargs)
        context = dict()

        # ensure guards is a sequence for following for-loop
        if not is_sequence(guards):
            guards = [guards]
        # execute each Guard, raising
        # NotAuthorized as soon as possible
        for guard in guards:
            ok = guard(context, arguments)
            if not ok:
                raise GuardFailed(guard)

    def _compute_arguments_dict(self, endpoint, args, kwargs) -> Dict:
        """
        Merge all args and kwargs into a single Dict.
        """
        arguments = dict(
            zip([k for k in endpoint.signature.parameters][:len(args)], args)
        )
        arguments.update(kwargs)
        return arguments
