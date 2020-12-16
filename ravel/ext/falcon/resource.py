from typing import Text, Dict

from .constants import HTTP_METHODS, RE_HANDLER_METHOD


class FalconResource(object):
    def __init__(self, route: str = None):
        self._route = route
        self._method_2_endpoint = {}

    def __getattr__(self, key) -> 'Endpoint':
        """
        This simulates falcon resource methods, like on_get,
        on_post, etc, when called on the instance.
        """
        match = RE_HANDLER_METHOD.match(key)
        if match is not None:
            method = match.groups()[0].upper()
            if not self.is_method_supported(method):
                raise AttributeError(
                    f'HTTP {method.upper()} unsupported '
                    f'for URL path {self._route}'
                )
            endpoint = self.get_endpoint(method)
            return endpoint

        raise AttributeError(key)

    @property
    def route(self) -> str:
        return self._route

    def get_method_map(self) -> Dict[Text, 'Endpoint']:
        return self._method_2_endpoint.copy()

    def get_endpoint(self, method: Text) -> 'Endpoint':
        return self._method_2_endpoint.get(method.upper())

    def add_endpoint(self, endpoint: 'Endpoint'):
        # TODO: Raise exception regarding already registered to http method
        self._method_2_endpoint[endpoint.method.upper()] = endpoint

    def is_method_supported(self, method: Text) -> bool:
        is_recognized = method.upper() in HTTP_METHODS
        is_registered = method.upper() in self._method_2_endpoint
        return (is_recognized and is_registered)
