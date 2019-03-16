import inspect

from inspect import Parameter
from typing import Dict, Set, Tuple, Type, Text, List

from appyratus.enum import Enum
from pybiz.exc import NotAuthorizedError
from pybiz.util import is_sequence

from .base import RegistryMiddleware

# Op codes used by CompositeAuthCallback:
OP_CODE = Enum(
    AND='&',
    OR='|',
    NOT='~',
)


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


class AuthCallback(object):
    """
    Subclasses of AuthCallback must implement on_authorization, which determines
    whether a given request is authorized, by inspecting arguments passed into
    a corresponding RegistryProxy at runtime.

    Positional and keyword argument names declared in the on_authorization
    method are plucked from the incoming arguments dynamically (following the
    required `context` dict argument).

    The context dict is shared by all AuthCallbacks composed in a
    CompositeAuthCallback boolean expression.
    """

    def __init__(self):
        self.spec = ArgumentSpecification(self)

    def __call__(self, context: Dict, arguments: Dict) -> bool:
        args, kwargs = self.spec.extract(arguments)
        return self.on_authorization(context, *args, **kwargs)

    def __and__(self, other):
        return CompositeAuthCallback(OP_CODE.AND, self, other)

    def __or__(self, other):
        return CompositeAuthCallback(OP_CODE.OR, self, other)

    def __invert__(self):
        return CompositeAuthCallback(OP_CODE.NOT, self, None)

    def on_authorization(self, context: Dict, *args, **kwargs) -> bool:
        """
        Determine whether RegistryProxy request is authorized by performing any
        necessary authorization check here. Each subclass must explicitly
        declare which arguments are required. For example,

        ```python
        class UserOwnsPost(AuthCallback):
            def on_authorization(context, user, post):
                return user.owns(post)
        ```

        This implementation expects to be used with a RegistryProxy with "user"
        and "post" arguments, for instance:

        ```python3
        @repl(auth=UserOwnsPost())
        def delete_post(user, post):
            post.delete()
        ```

        It is possible to combine AuthCallbacks in boolean expressions, using
        '&' (AND), '|' (OR) and '~' (NOT), like

        ```python3
        @repl(auth=(UserOwnsPost() | UserIsAdmin()))
        def delete_post(user, post):
            post.delete()
        ```
        """
        raise NotImplemented('override in subclass')


class CompositeAuthCallback(AuthCallback):
    """
    A CompositeAuthCallback represents a boolean expression involving one or
    more AuthCallback, which can themselves be other CompositeAuthCallback. This
    subclass is used to form logical predicates involving multiple
    AuthCallbacks.
    """

    def __init__(self, op: Text, lhs: AuthCallback, rhs: AuthCallback):
        self._op = op
        self._lhs = lhs
        self._rhs = rhs

    def __call__(self, context: Dict, arguments: Dict) -> bool:
        return self.on_authorization(context, arguments)

    def on_authorization(self, context: Dict, arguments: Dict):
        """
        Compute the boolean value of one or more nested AuthCallback in a
        depth-first manner.
        """
        is_authorized = False    # retval

        # compute LHS for both & and |.
        lhs_is_authorized = self._lhs(context, arguments)

        if self._op == OP_CODE.AND:
            # We only need to check RHS if LHS isn't already False.
            if lhs_is_authorized:
                rhs_is_authorized = self._rhs(context, arguments)
                is_authorized = (lhs_is_authorized and rhs_is_authorized)
        elif self._op == OP_CODE.OR:
            rhs_is_authorized = self._rhs(context, arguments)
            is_authorized = (lhs_is_authorized or rhs_is_authorized)
        elif self._op == OP_CODE.NOT:
            is_authorized = (not lhs_is_authorized)
        else:
            raise ValueError(f'op not recognized, "{self._op}"')

        return is_authorized


class AuthCallbackMiddleware(RegistryMiddleware):
    """
    Apply the AuthCallback(s) associated with a proxy, set via the `auth`
    RegistryDecorator keyword argument, e.g., repl(auth=IsFoo()).
    NotAuthorizedError is raised if not authorized.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def on_request(self, proxy: 'RegistryProxy', args: Tuple, kwargs: Dict):
        """
        In on_request, args and kwargs are in the form output by
        registry.on_request.
        """
        callbacks = proxy.auth
        if not callbacks:
            return
        arguments = self._compute_arguments_dict(proxy, args, kwargs)
        context = dict()

        # ensure callbacks is a sequence for following for-loop
        if not is_sequence(callbacks):
            callbacks = [callbacks]
        # execute each AuthCallback, raising
        # NotAuthorizedError as soon as possible
        for authorize in callbacks:
            is_authorized = authorize(context, arguments)
            if not is_authorized:
                raise NotAuthorizedError()

    def _compute_arguments_dict(self, proxy, args, kwargs) -> Dict:
        """
        Merge all args and kwargs into a single Dict.
        """
        arguments = dict(
            zip([k for k in proxy.signature.parameters][:len(args)], args)
        )
        arguments.update(kwargs)
        return arguments
