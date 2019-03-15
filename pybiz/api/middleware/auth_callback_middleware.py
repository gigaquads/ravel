from typing import Dict, Set, Tuple, Type

from pybiz.exc import NotAuthorizedError

from .base import RegistryMiddleware


class AuthCallback(object):
    def __call__(self, arguments: Dict) -> bool:
        self.arguments = arguments
        print('>>> CALLING', self)
        return self.on_call()

    def on_call(self) -> bool:
        raise NotImplemented('override in subclass')


class AuthCallbackMiddleware(RegistryMiddleware):
    def on_request(self, proxy: 'RegistryProxy', args: Tuple, kwargs: Dict):
        """
        In on_request, args and kwargs are in the form output by
        registry.on_request.
        """
        arguments = dict(
            zip([k for k in proxy.signature.parameters][:len(args)], args)
        )
        arguments.update(kwargs)
        callables = proxy.auth
        if callables is None:
            return
        if not callables:
            return
        if not isinstance(callables, list):
            callables = [callables]
        for c in callables:
            print(c, arguments)
            is_authorized = c(arguments)
            if not is_authorized:
                raise NotAuthorizedError()
