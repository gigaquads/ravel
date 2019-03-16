import inspect

from inspect import Parameter
from typing import Dict, Tuple, List


class ArgumentSpecification(object):
    """
    ArgumentSpecification determines which positional and keyword arguments a
    given AuthCallback needs. AuthCallbackMiddleware and CompositeAuthCallback
    use this information to know which incoming proxy arguments should be bound
    to the arguments declared by the corresponding AuthCallback.on_authorization
    method.
    """

    def __init__(self, callback: 'AuthCallback'):
        self.callback = callback
        self.signature = inspect.signature(callback.on_authorization)

        # determine which arguments expected by the callback's
        # on_authorization method that are positional and which are keyword.
        self.kwarg_keys = set()
        self.arg_keys = []
        self.arg_key_set = set()
        for k, param in self.signature.parameters.items():
            if k == 'context':
                continue
            if param.kind != Parameter.POSITIONAL_OR_KEYWORD:
                break
            if param.default is Parameter.empty:
                self.arg_keys.append(k)
                self.arg_key_set.add(k)
            else:
                self.kwarg_keys.add(k)

        self.has_var_kwargs = False
        if 'kwargs' in self.signature.parameters:
            param = self.signature.parameters['kwargs']
            self.has_var_kwargs = param.kind == Parameter.VAR_KEYWORD

        self.has_var_args = False
        if 'args' in self.signature.parameters:
            param = self.signature.parameters['args']
            self.has_var_args = param.kind == Parameter.VAR_POSITIONAL


    def extract(self, arguments: Dict) -> Tuple[List, Dict]:
        """
        Partition arguments between a list of position arguments and a dict
        of keyword arguments.
        """
        args = [arguments[k] for k in self.arg_keys]
        if self.has_var_kwargs:
            kwargs = {
                k: v for k, v in arguments.items()
                if k not in self.arg_key_set
            }
        else:
            kwargs = {k: arguments[k] for k in self.kwarg_keys}
        return (args, kwargs)
