from typing import Dict, Tuple, Type, Text

from .base import RegistryMiddleware


class HttpSessionMiddleware(RegistryMiddleware):

    def __init__(self, cookie='sid', query=None):
        super().__init__()
        self._cookie = cookie
        self._query = query

    @property
    def registry_types(self) -> Tuple[Type['Registry']]:
        from pybiz.api.http import HttpRegistry

        return (HttpRegistry, )

    def pre_request(self, proxy, raw_args, raw_kwargs):
        request = raw_args[0]

        if 'session' in proxy.signature.parameters:
            cookie_value = request.cookies.get(self._cookie)
            if cookie_value is not None:
                raw_kwargs[session] = self._query(cookie_value)
