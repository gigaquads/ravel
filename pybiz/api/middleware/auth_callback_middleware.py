from typing import Dict, Tuple, Set, Type

from .base import RegistryMiddleware


class AuthCallbackMiddleware(RegistryMiddleware):
    def on_request(self, proxy: 'RegistryProxy', args: Tuple, kwargs: Dict):
        """
        In on_request, args and kwargs are in the form output by
        registry.on_request.
        """
        if proxy.auth is not None and callable(proxy.auth):
            arguments = zip(proxy.signature.parameters[:len(args)], args)
            arguments.update(kwargs)
            is_authorized = proxy.auth(arguments)
            if not is_authorized:
                raise Exception('not authorized')
