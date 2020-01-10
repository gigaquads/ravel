from typing import Dict, Text, Callable

from appyratus.enum import Enum

from pybiz.util.misc_functions import get_class_name

from .exceptions import GuardFailure
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
    a corresponding Endpoint at runtime.

    Positional and keyword argument names declared in the guard
    method are plucked from the incoming arguments dynamically (following the
    required `context` dict argument).

    The context dict is shared by all Guards composed in a
    BooleanGuard boolean expression.
    """

    def __init__(self, execute: Callable = None, parent: 'Guard' = None):
        self.parent = parent
        self.callback = execute
        if self.callback is not None:
            self.spec = ArgumentSpecification(self.callback)
        else:
            self.spec = ArgumentSpecification(self)

    def __repr__(self):
        return f'{get_class_name(self)}(description=\'{self.description}\')'

    def __call__(self, context: Dict, arguments: Dict) -> bool:
        args, kwargs = self.spec.extract(arguments)
        return self.execute(context, *args, **kwargs)

    def __and__(self, other) -> 'BooleanGuard':
        boolean_guard = BooleanGuard(OP_CODE.AND, self, other)
        self.parent = boolean_guard
        other.parent = boolean_guard
        return boolean_guard

    def __or__(self, other) -> 'BooleanGuard':
        boolean_guard = BooleanGuard(OP_CODE.OR, self, other)
        self.parent = boolean_guard
        other.parent = boolean_guard
        return boolean_guard

    def __invert__(self) -> 'BooleanGuard':
        boolean_guard = BooleanGuard(OP_CODE.NOT, self, None)
        self.parent = boolean_guard
        other.parent = boolean_guard
        return boolean_guard

    def fail(self, message=None) -> GuardFailure:
        return GuardFailure(self, message=message, traceback_depth=1)

    @property
    def description(self) -> Text:
        return get_class_name(self)

    @property
    def root(self) -> 'Guard':
        child = self
        while True:
            if child.parent is None:
                return child
            else:
                child = child.parent

    def execute(self, context: Dict, *args, **kwargs) -> bool:
        """
        Determine whether Endpoint request is authorized by performing any
        necessary authorization check here. Each subclass must explicitly
        declare which arguments are required. For example,

        ```python
        class UserOwnsPost(Guard):
            def execute(context, user, post):
                return user.owns(post)
        ```

        This implementation expects to be used with a Endpoint with "user"
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
        if self.callback is None:
            raise NotImplemented('override in subclass')
        return self.callback(context, *args, **kwargs)


class BooleanGuard(Guard):
    """
    A BooleanGuard represents a boolean expression involving one or
    more Guard, which can themselves be other BooleanGuard. This
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
    def op_code(self):
        return self._op

    @property
    def lhs(self):
        return self._lhs

    @property
    def rhs(self):
        return self._rhs

    @property
    def description(self):
        if self._op == OP_CODE.NOT:
            return f'NOT ({self._lhs.description})'
        if self._op == OP_CODE.AND:
            return f'({self._lhs.description} AND {self._rhs.description})'
        if self._op == OP_CODE.OR:
            return f'({self._lhs.description} OR {self._rhs.description})'

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
