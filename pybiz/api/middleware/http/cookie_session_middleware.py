from typing import Dict, Tuple, Type, Text, List
from inspect import Parameter

import pybiz.api.web

from ..base import ApiMiddleware


class CookieSessionMiddleware(ApiMiddleware):
    """
    This middleware will try to extract a session ID from an HTTP cookie header
    and load the corresponding Session business object into the raw kwargs
    available to the proxy.
    """

    def __init__(
        self,
        session_type_name: Text = 'Session',
        session_arg_name: Text = 'session',
        cookie_name='sid',
    ):
        super().__init__()
        self._session_type_name = session_type_name
        self._cookie_name = cookie_name
        self._arg_name = session_arg_name

    @property
    def api_types(self) -> Tuple[Type['Api']]:
        return (pybiz.api.web.Http, )

    def on_request(self, proxy: 'ApiProxy', args: List, kwargs: Dict):
        request = self.get_request()
        if request is None:
            return

        sess_param = proxy.signature.parameters.get(self._arg_name)
        if sess_param is None:
            return

        session_id = self.get_session_id(request)
        if session_id is None:
            return

        # first try to bind session to a kwarg, if present, O(1). Otherwise,
        # scan the positional arugments, O(N)
        if sess_param.default != Parameter.empty:
            raw_kwargs[session] = self.get_session(session_id)
        else:
            for idx, name in enumerate(proxy.signature.parameter):
                if name == session_arg_name:
                    raw_args[idx] = self.get_session(session_id)
                    break

    def get_request(self, raw_args, raw_kwargs):
        return raw_args[0]

    def get_session_id(self, request):
        return request.cookies.get(self._cookie_name)

    def get_session(self, session_id):
        session_type = self.api.types.biz.get(self._session_type_name)
        if session_type is not None:
            return session_type.get(session_id)
        return None
