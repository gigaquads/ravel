from typing import Dict, Tuple, Type, Text, List, Callable
from inspect import Parameter

import pybiz.app.web

from ..application_middleware import ApplicationMiddleware


class CookieSessionMiddleware(ApplicationMiddleware):
    """
    This middleware will try to extract a session ID from an HTTP cookie header
    and load the corresponding Session business object into the raw kwargs
    available to the endpoint.
    """

    def __init__(
        self,
        session_class_name: Text = 'Session',
        session_arg_name: Text = 'session',
        request_getter: Callable = None,
        cookie_name='sid',
    ):
        super().__init__()
        self._session_class_name = session_class_name
        self._cookie_name = cookie_name
        self._arg_name = session_arg_name
        self._request_getter = request_getter or self.get_request

    @property
    def app_types(self) -> Tuple[Type['Application']]:
        return (pybiz.app.web.Http, )

    def pre_request(self, endpoint: 'Endpoint', raw_args: List, raw_kwargs: Dict):
        request = self._request_getter(raw_args, raw_kwargs)
        if request is None:
            return

        sess_param = endpoint.signature.parameters.get(self._arg_name)
        if sess_param is None:
            return

        session_id = self.get_session_id(request)
        if session_id is None:
            return

        # first try to bind session to a kwarg, if present, O(1). Otherwise,
        # scan the positional arugments, O(N)
        if sess_param.default != Parameter.empty:
            raw_kwargs[self._arg_name] = self.get_session(session_id)
        else:
            for name in endpoint.signature.parameters:
                if name == self._arg_name:
                    raw_kwargs[name] = self.get_session(session_id)
                    break

    def get_request(self, raw_args, raw_kwargs):
        return raw_args[0]

    def get_session_id(self, request):
        return request.cookies.get(self._cookie_name)

    def get_session(self, session_id):
        session_class = self.app.biz.get(self._session_class_name)
        if session_class is not None:
            return session_class.get(session_id)
        return None
