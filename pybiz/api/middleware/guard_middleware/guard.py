from typing import Dict, Text

from appyratus.enum import Enum

from pybiz.exc import ApiError


from .argument_specification import ArgumentSpecification

OP_CODE = Enum(
    AND='&',
    OR='|',
    NOT='~',
)


class Guard(object):
    """
    Subclasses of Guard must implement on_authorization, which determines
    whether a given request is authorized, by inspecting arguments passed into
    a corresponding RegistryProxy at runtime.

    Positional and keyword argument names declared in the on_authorization
    method are plucked from the incoming arguments dynamically (following the
    required `context` dict argument).

    The context dict is shared by all Guards composed in a
    CompositeGuard boolean expression.
    """

    def __init__(self):
        self.spec = ArgumentSpecification(self)

    def __call__(self, context: Dict, arguments: Dict) -> Exception:
        args, kwargs = self.spec.extract(arguments)
        return self.on_authorization(context, *args, **kwargs)

    def __and__(self, other):
        return CompositeGuard(OP_CODE.AND, self, other)

    def __or__(self, other):
        return CompositeGuard(OP_CODE.OR, self, other)

    def __invert__(self):
        return CompositeGuard(OP_CODE.NOT, self, None)

    def on_authorization(self, context: Dict, *args, **kwargs) -> bool:
        """
        Determine whether RegistryProxy request is authorized by performing any
        necessary authorization check here. Each subclass must explicitly
        declare which arguments are required. For example,

        ```python
        class UserOwnsPost(Guard):
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

        It is possible to combine Guards in boolean expressions, using
        '&' (AND), '|' (OR) and '~' (NOT), like

        ```python3
        @repl(auth=(UserOwnsPost() | UserIsAdmin()))
        def delete_post(user, post):
            post.delete()
        ```
        """
        raise NotImplemented('override in subclass')


class CompositeGuard(Guard):
    """
    A CompositeGuard represents a boolean expression involving one or
    more Guard, which can themselves be other CompositeGuard. This
    subclass is used to form logical predicates involving multiple
    Guards.
    """

    def __init__(self, op: Text, lhs: Guard, rhs: Guard):
        self._op = op
        self._lhs = lhs
        self._rhs = rhs

    def __call__(self, context: Dict, arguments: Dict) -> bool:
        return self.on_authorization(context, arguments)

    def on_authorization(self, context: Dict, arguments: Dict):
        """
        Compute the boolean value of one or more nested Guard in a
        depth-first manner.
        """
        is_authorized = False    # retval

        # compute LHS for both & and |.
        lhs_exc = self._lhs(context, arguments)

        if self._op == OP_CODE.AND:
            # We only need to check RHS if LHS isn't already False.
            if lhs_exc is None:
                rhs_exc = self._rhs(context, arguments)
                if rhs_exc is not None:
                    return rhs_exc
            else:
                return lhs_exc
        elif self._op == OP_CODE.OR:
            rhs_exc = self._rhs(context, arguments)
            if rhs_exc is not None and lhs_exc is not None:
                return CompositeGuardException(lhs_exc, rhs_exc)
        elif self._op == OP_CODE.NOT:
            if lhs_exc is not None:
                return lhs_exc
        else:
            return ValueError(f'op not recognized, "{self._op}"')

        return None

class CompositeGuardException(ApiError):
    def __init__(self, lhs_exc, rhs_exc):
        message = (
            f'{lhs_exc.__class__.__name__}: {lhs_exc.message}\n'
            f'{rhs_exc.__class__.__name__}: {rhs_exc.message}'
        )
        super().__init__(message)
