from typing import Dict, Tuple, Type, Text

from .registry_middleware import RegistryMiddleware


class HttpSessionMiddleware(RegistryMiddleware):

    def __init__(
        self,
        session_type: Type['BizObject'],
        cookie_name: Text='sid'
    ):
        super().__init__()
        self._session_type = session_type
        self._cookie_name = cookie_name

    @property
    def registry_types(self) -> Tuple[Type['Registry']]:
        from pybiz.api.http import HttpRegistry
        return (HttpRegistry, )

    def pre_request(self, proxy, args, kwargs):
        req = args[0]
        needs_session = 'session' in proxy.signature.parameters
        if needs_session:
            session_id = req.cookies.get(self._cookie_name)
            if session_id is not None:
                session_id = int(session_id)
                kwargs['session'] = self._session_type.get(
                    _id=session_id,
                    fields={'*', 'user.*'}
                )
