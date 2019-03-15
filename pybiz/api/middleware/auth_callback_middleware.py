from typing import Dict, Set, Tuple, Type, Text

from pybiz.exc import NotAuthorizedError

from .base import RegistryMiddleware


class AuthCallback(object):
    def __call__(self, arguments: Dict, context: Dict = None) -> bool:
        return self.on_authorization(arguments, context)

    def on_authorization(self, arguments: Dict = None, context: Dict) -> bool:
        raise NotImplemented('override in subclass')

    def __and__(self, other):
        return CompositeAuthCallback('&', self, other)

    def __or__(self, other):
        return CompositeAuthCallback('|', self, other)


class CompositeAuthCallback(AuthCallback):
    def __init__(self, op: Text, lhs: AuthCallback, rhs: AuthCallback):
        pass

    def on_authorization(self, arguments: Dict = None, context:  Dict = None):
        if op == '&':
            return self.lhs(context) and self.rhs(context)
        elif op == '|':
            return self.lhs(context) or self.rhs(context)
        else:
            raise ValueError('op not recognized')


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
        if not callables:
            return
        if not isinstance(callables, list):
            callables = [callables]
        context = dict()
        for func in callables:
            print(func, arguments, context)
            is_authorized = func(arguments=arguments, context=context)
            if not is_authorized:
                raise NotAuthorizedError()
