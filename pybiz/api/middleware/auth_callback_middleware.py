from typing import Dict, Set, Tuple, Type, Text

from pybiz.exc import NotAuthorizedError

from .base import RegistryMiddleware


class AuthCallback(object):
    def __call__(self, arguments: Dict = None, context: Dict = None) -> bool:
        return self.on_authorization(arguments, context)

    def on_authorization(
        self, arguments: Dict = None, context: Dict = None
    ) -> bool:
        raise NotImplemented('override in subclass')

    def __and__(self, other):
        return CompositeAuthCallback('&', self, other)

    def __or__(self, other):
        return CompositeAuthCallback('|', self, other)


class CompositeAuthCallback(AuthCallback):
    def __init__(self, op: Text, lhs: AuthCallback, rhs: AuthCallback):
        self._op = op
        self._lhs = lhs
        self._rhs = rhs

    def on_authorization(self, arguments: Dict = None, context: Dict = None):
        if self._op == '&':
            return self._lhs(arguments, context) and self._rhs(arguments, context)
        elif self._op == '|':
            return self._lhs(arguments, context) or self._rhs(arguments, context)
        else:
            raise ValueError(f'op not recognized, "{self._op}"')


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
            is_authorized = func(arguments=arguments, context=context)
            if not is_authorized:
                raise NotAuthorizedError()
