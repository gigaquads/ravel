from typing import Text, Tuple, List, Set, Dict, Type, Union, Callable
from random import randint

from ravel.util.loggers import console
from ravel.util.misc_functions import get_class_name, flatten_sequence
from ravel.query.predicate import (
    Predicate,
    ConditionalPredicate,
    BooleanPredicate,
    OP_CODE,
)


from ravel.util import is_resource, is_batch
from ravel.query.order_by import OrderBy
from ravel.query.request import Request
from ravel.resolver.resolver import Resolver
from ravel.resolver.resolver_property import ResolverProperty


class Loader(Resolver):
    def __init__(self, field, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.field = field

    @classmethod
    def property_type(cls):
        return LoaderProperty

    @classmethod
    def tags(cls) -> Set[Text]:
        return {'fields'}

    @classmethod
    def priority(cls) -> int:
        return 1

    def on_resolve(self, resource, request):
        exists_resource = resource._id is not None
        if not exists_resource:
            return None

        unloaded_field_names = list(
            resource.Schema.fields.keys() - resource.internal.state.keys()
        )
        state = resource.store.dispatch('fetch', kwargs={
            'fields': unloaded_field_names
        })
        if state is not None:
            resource.merge(state)

        return state[request.resolver.field.name]

    def on_simulate(self, resource, request):
        value = None

        if self.nullable:
            if randint(1, 10) > 1:
                value = self.field.generate()
        else:
            value = self.field.generate()

        return value

    def on_backfill(self, resource, request):
        return self.on_simulate(resource, request)


class LoaderProperty(ResolverProperty):
    def __hash__(self):
        return super().__hash__()

    def __repr__(self):
        return f'{get_class_name(self.resolver.owner)}.{self.resolver.name}'

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
        others = {obj._id if is_resource(obj) else obj for obj in others}
        return ConditionalPredicate(
            OP_CODE.INCLUDING, self, others, is_scalar=False
        )

    def excluding(self, *others) -> Predicate:
        others = flatten_sequence(others)
        others = {obj._id if is_resource(obj) else obj for obj in others}
        return ConditionalPredicate(
            OP_CODE.EXCLUDING, self, others, is_scalar=False
        )

    def fset(self, owner: 'Resource', value):
        field = self.resolver.field
        if value is None and field.nullable:
            processed_value = None
        else:
            processed_value, errors = field.process(value)
            if errors:
                raise Exception('ValidationError: ' + str(errors))
        super().fset(owner, processed_value)

    @property
    def asc(self):
        return OrderBy(self.resolver.field.name, desc=False)

    @property
    def desc(self):
        return OrderBy(self.resolver.field.name, desc=True)
