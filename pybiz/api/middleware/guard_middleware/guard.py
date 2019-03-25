from typing import Dict, Text

from appyratus.enum import Enum

from pybiz.api.exc import GuardFailed

from .argument_specification import ArgumentSpecification


OP_CODE = Enum(
    AND='&',
    OR='|',
    NOT='~',
)


class Guard(object):
    """
    Subclasses of Guard must implement guard, which determines
    whether a given request is authorized, by inspecting arguments passed into
    a corresponding RegistryProxy at runtime.

    Positional and keyword argument names declared in the guard
    method are plucked from the incoming arguments dynamically (following the
    required `context` dict argument).

    The context dict is shared by all Guards composed in a
    CompositeGuard boolean expression.
    """

    def __init__(self):
        self.spec = ArgumentSpecification(self)

    def __repr__(self):
        return f'<Guard({self.display_string})>'

    def __call__(self, context: Dict, arguments: Dict) -> bool:
        args, kwargs = self.spec.extract(arguments)
        return self.execute(context, *args, **kwargs)

    def __and__(self, other) -> 'CompositeGuard':
        return CompositeGuard(OP_CODE.AND, self, other)

    def __or__(self, other) -> 'CompositeGuard':
        return CompositeGuard(OP_CODE.OR, self, other)

    def __invert__(self) -> 'CompositeGuard':
        return CompositeGuard(OP_CODE.NOT, self, None)

    @property
    def display_string(self) -> Text:
        return f'{self.__class__.__name__}'

    def execute(self, context: Dict, *args, **kwargs) -> bool:
        """
        Determine whether RegistryProxy request is authorized by performing any
        necessary authorization check here. Each subclass must explicitly
        declare which arguments are required. For example,

        ```python
        class UserOwnsPost(Guard):
            def execute(context, user, post):
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
        super().__init__()
        self._op = op
        self._lhs = lhs
        self._rhs = rhs

    def __call__(self, context: Dict, arguments: Dict) -> bool:
        return self.execute(context, arguments)

    @property
    def display_string(self):
        if self._op == OP_CODE.NOT:
            return f'~{self._lhs.display_string}'
        if self._op == OP_CODE.AND:
            return f'({self._lhs.display_string} & {self._rhs.display_string})'
        if self._op == OP_CODE.OR:
            return f'({self._lhs.display_string} | {self._rhs.display_string})'

    def execute(self, context: Dict, arguments: Dict):
        """
        Compute the boolean value of one or more nested Guard in a
        depth-first manner.
        """
        is_authorized = False    # retval

        # compute LHS for both & and |.
        lhs_ok = self._lhs(context, arguments)

        if self._op == OP_CODE.AND:
            # We only need to check RHS if LHS isn't already False.
            if lhs_ok:
                rhs_ok = self._rhs(context, arguments)
                if not rhs_ok:
                    raise GuardFailed(self._rhs)
            else:
                raise GuardFailed(self._lhs)
        elif self._op == OP_CODE.OR:
            if not lhs_ok:
                rhs_ok = self._rhs(context, arguments)
                if not rhs_ok:
                    raise GuardFailed(self)
        elif self._op == OP_CODE.NOT:
            if lhs_ok:
                raise GuardFailed(self)

        return True
