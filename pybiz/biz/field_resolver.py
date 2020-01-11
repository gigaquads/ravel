from typing import Text, Set, Dict, List, Callable, Type, Tuple

from pybiz.util.loggers import console
from pybiz.util.misc_functions import (
    is_sequence,
    flatten_sequence,
)
from pybiz.constants import ID_FIELD_NAME
from pybiz.predicate import (
    Predicate,
    ConditionalPredicate,
    BooleanPredicate,
    OP_CODE,
)

from .util import is_biz_object
from .query.order_by import OrderBy
from .resolver.resolver import Resolver
from .resolver.resolver_property import ResolverProperty


class FieldResolver(Resolver):
    def __init__(self, field, *args, **kwargs):
        super().__init__(
            target_biz_class=kwargs.get('biz_class'),
            private=field.meta.get('private', False),
            required=field.required,
            *args, **kwargs
        )
        self.field = field
        if field.name is None:
            field.name = self.name

    @property
    def asc(self) -> 'OrderBy':
        return OrderBy(self.field.source, desc=False)

    @property
    def desc(self) -> 'OrderBy':
        return OrderBy(self.field.source, desc=True)

    @property
    def lazy(self) -> bool:
        return self._lazy

    @classmethod
    def tags(cls) -> Set[Text]:
        return {'fields'}

    @classmethod
    def priority(cls) -> int:
        return 1

    def on_bind(self, biz_class: Type['BizObject']):
        """
        For FieldResolvers, the target is this owner BizObject class, since the
        field value comes from it, not some other type, as with Relationships,
        for instance.
        """
        self.target_biz_class = biz_class

    @staticmethod
    def on_execute(
        owner: 'BizObject',
        resolver: 'Resolver',
        request: 'QueryRequest'
    ):
        """
        Return the field value from the owner object's state dict. Lazy load the
        field if necessary.
        """
        owner_id = owner.internal.state.get(ID_FIELD_NAME)
        if owner_id is None:
            return None

        field_name = resolver.field.name

        # lazy load this field and any other lazily loaded field
        request.query.select(field_name)
        request.query.select(
            k for k, r in owner.pybiz.resolvers.fields.items()
            if k not in owner.internal.state
        )

        field_values = owner.dao.dispatch(
            method_name='fetch',
            args=(owner_id, ),
            kwargs={'fields': request.query.params.select.keys()}
        )

        owner.merge(field_values)

        return field_values[field_name]

    def dump(self, dumper: 'Dumper', value):
        """
        Run the raw value stored in the state dict through the corresponding
        Field object's process method, which validates and possibly transforms
        the data somehow, depending on how the Field was declared.
        """
        return value

    def generate(self, owner: 'BizObject', query: 'ResolverQuery'):
        if query.parent and query.parent.params.where:
            pass # TODO
        else:
            return self.field.generate()


class FieldResolverProperty(ResolverProperty):

    def __hash__(self):
        return super().__hash__()

    def __eq__(self, other: Predicate) -> Predicate:
        return ConditionalPredicate(OP_CODE.EQ, self, other)

    def __ne__(self, other: Predicate) -> Predicate:
        return ConditionalPredicate(OP_CODE.NEQ, self, other)

    def __lt__(self, other: Predicate) -> Predicate:
        return ConditionalPredicate(OP_CODE.LT, self, other)

    def __le__(self, other: Predicate) -> Predicate:
        return ConditionalPredicate(OP_CODE.LEQ, self, other)

    def __gt__(self, other: Predicate) -> Predicate:
        return ConditionalPredicate(OP_CODE.GT, self, other)

    def __ge__(self, other: Predicate) -> Predicate:
        return ConditionalPredicate(OP_CODE.GEQ, self, other)

    def including(self, *others) -> Predicate:
        others = flatten_sequence(others)
        others = {obj._id if is_biz_object(obj) else obj for obj in others}
        return ConditionalPredicate(OP_CODE.INCLUDING, self, others)

    def excluding(self, *others) -> Predicate:
        others = flatten_sequence(others)
        others = {obj._id if is_biz_object(obj) else obj for obj in others}
        return ConditionalPredicate(OP_CODE.EXCLUDING, self, others)

    def on_set(self, owner: 'BizObject', value):
        if value is None and self.field.nullable:
            processed_value = None
        else:
            processed_value, errors = self.field.process(value)
            if errors:
                raise Exception('ValidationError: ' + str(errors))
        super().on_set(owner, processed_value)

    @property
    def asc(self):
        return OrderBy(self.resolver.field.name, desc=False)

    @property
    def desc(self):
        return OrderBy(self.resolver.field.name, desc=True)

    @property
    def field(self):
        return self.resolver.field
