from typing import Dict, Tuple, Type, Text, List, Callable
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
        request_getter: Callable = None,
        cookie_name='sid',
    ):
        super().__init__()
        self._session_type_name = session_type_name
        self._cookie_name = cookie_name
        self._arg_name = session_arg_name
        self._request_getter = request_getter or self.get_request

    @property
    def api_types(self) -> Tuple[Type['Api']]:
        return (pybiz.api.web.Http, )

    def pre_request(self, proxy: 'ApiProxy', raw_args: List, raw_kwargs: Dict):
        request = self._request_getter(raw_args, raw_kwargs)
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
            raw_kwargs[self._arg_name] = self.get_session(session_id)
        else:
            for name in proxy.signature.parameters:
                if name == self._arg_name:
                    try:
                        raw_kwargs[name] = self.get_session(session_id)
                    except:
                        import ipdb; ipdb.set_trace()
                    break

    def get_request(self, raw_args, raw_kwargs):
        return raw_args[0]

    def get_session_id(self, request):
        return request.cookies.get(self._cookie_name)

    def get_session(self, session_id):
        session_type = self.api.biz.get(self._session_type_name)
        if session_type is not None:
            return session_type.get(session_id)
        return None
