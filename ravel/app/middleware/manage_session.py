from typing import Callable, Tuple, Dict

from .middleware import Middleware


class ManageSession(Middleware):
    def __init__(self, getter: Callable = None, setter: Callable = None):
        self._getter = getter
        self._setter = setter

    def pre_request(
        self,
        action: 'Action',
        request: 'Request',
        raw_args: Tuple,
        raw_kwargs: Dict
    ):
        if self._getter is not None:
            request.context.session = self._getter(request)

    def post_request(
        self,
        action: 'Action',
        request: 'Request',
        result,
    ):
        if self._setter is not None:
            self._setter(request)
        if request.session is not None:
            request.session.save()
