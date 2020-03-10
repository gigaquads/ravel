from typing import Callable, Tuple, Dict

from .middleware import Middleware


class GetSession(Middleware):
    def __init__(self, loader: Callable):
        self._loader = loader

    def pre_request(
        self,
        action: 'Action',
        request: 'Request',
        raw_args: Tuple,
        raw_kwargs: Dict
    ):
        request.context.session = self._loader(request)
