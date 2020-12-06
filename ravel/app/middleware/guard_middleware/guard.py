from typing import Dict, Text, Callable

from appyratus.enum import Enum

from ravel.util.misc_functions import get_class_name
from ravel.exceptions import RavelError

from .argument import ArgumentSpecification

OP_CODE = Enum(AND='AND', OR='OR', NOT='NOT')


class GuardFailure(RavelError):
    def __init__(self, guard, message=None, *args, **kwargs):
        super().__init__(message or 'guard failed', *args, **kwargs)
        self.guard = guard
        self.data['guard'] = {}
        self.data['guard']['class'] = get_class_name(guard)
        self.data['guard']['failed'] = guard.description
        if guard is not guard.root:
            self.data['guard']['description'] = guard.root.description


class Guard(object):
    """
    Subclasses of Guard must implement guard, which determines
    whether a given request is authorized, by inspecting arguments passed into
    a corresponding Action at runtime.

    Positional and keyword argument names declared in the guard
    method are plucked from the incoming arguments dynamically (following the
    required `request` dict argument).

    The request dict is shared by all Guards composed in a
    CompositeGuard boolean expression.
    """

    def __init__(
        self,
        execute: Callable = None,
        parent: 'Guard' = None,
        **kwargs
    ):
        self.parent = parent
        self.callback = execute
        self.kwargs = kwargs
        if self.callback is not None:
            self.spec = ArgumentSpecification(self.callback)
        else:
            self.spec = ArgumentSpecification(self)

    def __repr__(self):
        return f'{get_class_name(self)}(description=\'{self.description}\')'

    def __call__(self, request: Dict, arguments: Dict) -> bool:
        args, kwargs = self.spec.extract(arguments)
        return self.execute(request, *args, **kwargs)

    def __and__(self, other) -> 'CompositeGuard':
        composite_guard = CompositeGuard(OP_CODE.AND, self, other)
        self.parent = composite_guard
        other.parent = composite_guard
        return composite_guard

    def __or__(self, other) -> 'CompositeGuard':
        composite_guard = CompositeGuard(OP_CODE.OR, self, other)
        self.parent = composite_guard
        other.parent = composite_guard
        return composite_guard

    def __invert__(self) -> 'CompositeGuard':
        composite_guard = CompositeGuard(OP_CODE.NOT, self, None)
        self.parent = composite_guard
        other.parent = composite_guard
        return composite_guard

    def fail(self, message=None) -> GuardFailure:
        return GuardFailure(self, message=message, logged_traceback_depth=1)

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

    def execute(self, request: 'Request', *args, **kwargs) -> bool:
        """
        Determine whether Action request is authorized by performing any
        necessary authorization check here. Each subclass must explicitly
        declare which arguments are required. For example,

        ```python
        class UserOwnsPost(Guard):
            def execute(request, user, post):
                return user.owns(post)
        ```

        This implementation expects to be used with a Action with "user"
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
        return self.callback(request, *args, **kwargs)


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

    def __call__(self, request: 'Request', arguments: Dict) -> bool:
        return self.execute(request, arguments)

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
        opc = self._op
        if self._op == OP_CODE.NOT:
            return f'{opc} ({self._lhs.description})'
        if self._op == OP_CODE.AND:
            return f'({self._lhs.description} {opc} {self._rhs.description})'
        if self._op == OP_CODE.OR:
            return f'({self._lhs.description} {opc} {self._rhs.description})'

    def execute(self, request: 'Request', arguments: Dict):
        """
        Compute the boolean value of one or more nested Guard in a
        depth-first manner.
        """
        # compute LHS for both & and |.
        lhs_is_ok = self._lhs(request, arguments)

        if self._op == OP_CODE.AND:
            # We only need to check RHS if LHS isn't already False.
            if lhs_is_ok is False:
                raise GuardFailure(self._lhs)
            rhs_is_ok = self._rhs(request, arguments)
            if rhs_is_ok is False:
                raise GuardFailure(self._rhs)
        elif self._op == OP_CODE.OR:
            if lhs_is_ok is not False:
                rhs_is_ok = self._rhs(request, arguments)
                if rhs_is_ok is False:
                    raise GuardFailure(self)
        elif self._op == OP_CODE.NOT:
            if lhs_is_ok is not False:
                raise GuardFailure(self)

        return True
