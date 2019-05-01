from typing import Dict, Tuple, Type, Text
from inspect import Parameter

from .base import RegistryMiddleware


class HttpSessionMiddleware(RegistryMiddleware):
    """
    This middleware will try to extract a session ID from an HTTP cookie header
    and load the corresponding Session business object into the raw kwargs
    available to the proxy.
    """

    def __init__(self, session_type: Type['BizType'], cookie_name='sid'):
        super().__init__()
        self._session_type = session_type
        self._cookie_name = cookie_name

    @property
    def registry_types(self) -> Tuple[Type['Registry']]:
        from pybiz.api.http import HttpRegistry

        return (HttpRegistry, )

    def pre_request(
        self,
        proxy: 'RegistryProxy',
        raw_args: Tuple,
        raw_kwargs: Dict
    ):
        request = self.get_request()
        if request is not None:
            sess_param = proxy.signature.parameters.get('session')
            if sess_param is not None and sess_param.default != Parameter.empty:
                session_id = self.get_session_id(request)
                if session_id is not None:
                    raw_kwargs[session] = self.get_session(session_id)

    def get_request(self, raw_args, raw_kwargs):
        return raw_args[0]

    def get_session_id(self, request):
        return request.cookies.get(self._cookie_name)

    def get_session(self, session_id):
        return self._session_type.get(session_id)
