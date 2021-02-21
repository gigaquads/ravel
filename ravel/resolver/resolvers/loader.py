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


from ravel.constants import ID
from ravel.util import is_resource, is_batch
from ravel.query.order_by import OrderBy
from ravel.query.request import Request
from ravel.resolver.resolver import Resolver
from ravel.resolver.resolver_property import ResolverProperty
from ravel.schema import fields


class Loader(Resolver):
    """
    The Loader resolver is responsible for fetching Resource fields.
    """

    def __init__(self, field=None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._field = None
        if field:
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

    @property
    def field(self) -> 'Field':
        return self._field

    @property
    def is_virtual(self) -> bool:
        return bool(self.field.meta.get('ravel_on_resolve'))

    @field.setter
    def field(self, field: 'Field'):
        self._field = field
        self.nullable = field.nullable
        self.required = field.required
        self.private = field.meta.get('private', False)

    def on_copy(self, copy):
        copy.field = self.field

    def on_resolve(self, resource, request):
        exists_resource = ID in resource.internal.state
        if not exists_resource:
            return None

        # get all non-virtual field names in resource schema
        all_field_names = {
            f.name for f in resource.ravel.schema.fields.values()
            if not f.meta.get('ravel_on_resolve')
        }

        # of those, take only thos which are not yet loaded from the store.
        # these are the ones we are going to try to fetch.
        unloaded_field_names = (
            all_field_names - resource.internal.state.keys()
        )
        unloaded_field_names.add(self._field.name)
        
        # container for resolved resource instance state
        new_resource_state = {}

        # if the field has a on_resolve function, put there by the
        # @field decorator, use it instead of fetching from store.
        field_on_resolve = self._field.meta.get('ravel_on_resolve')
        if field_on_resolve is not None:
            unloaded_field_names.discard(self._field.name)
            value = field_on_resolve(resource, request)
            new_resource_state[self._field.name] = value

        # merge new state into existing resoruce instance state
        new_resource_state.update(
            resource.ravel.local.store.dispatch(
                'fetch',
                args=(resource._id, ),
                kwargs={'fields': unloaded_field_names.copy()}
            ) or {}
        )

        # merge in new state to existing resource, not overwriting
        # any fields that are dirty, i.e. have changes.
        keys_to_clean = set()
        for k, v in new_resource_state.items():
            is_dirty = k in resource.internal.state.dirty
            if not is_dirty or k in unloaded_field_names:
                keys_to_clean.add(k)
                resource[k] = v

        # mark the loaded fields as "clean", meaning, we are telling the system
        # that thse fields are new, not stale and in need of saving.
        resource.clean(keys_to_clean)

        return resource

    def on_resolve_batch(self, batch, request):
        id_2_resource = {res._id: res for res in batch}
        resource_ids = []
        for res in batch:
            id_2_resource[res._id] = res
            res_id = res.internal.state.get('_id')
            if res_id is not None:
                resource_ids.append(res_id)

        # field names to fetch (fetch all eagerly)
        field_names = set(self.target.ravel.schema.fields.keys())
        state_dicts = self.owner.ravel.local.store.dispatch('fetch_many',
            args=(resource_ids, ),
            kwargs={'fields': field_names}
        )

        for res_id, state in state_dicts.items():
            if state:
                id_2_resource[res_id].merge(state)

        return batch

    def on_simulate(self, resource, request):
        value = None

        if self.nullable and self._field.name != ID:
            # use None as the simulated value 10% of the time
            if randint(1, 10) > 1:
                value = self._field.generate()
        else:
            value = self._field.generate()

        return value

    def on_backfill(self, resource, request):
        return self.on_simulate(resource, request)


class View(Loader):

    @classmethod
    def tags(cls) -> Set[Text]:
        return {'view'}

    @classmethod
    def priority(cls) -> int:
        return 20

    def on_resolve(self, resource, request):
        raise NotImplementedError()


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
        visited = set()
        deduplicated = []
        for obj in others:
            if is_resource(obj):
                if obj._id not in visited:
                    visited.add(obj._id)
                    deduplicated.append(obj._id)
            else:
                if obj not in visited:
                    visited.add(obj)
                    deduplicated.append(obj)
                
        return ConditionalPredicate(OP_CODE.INCLUDING, self, deduplicated)

    def excluding(self, *others) -> Predicate:
        others = flatten_sequence(others)
        others = {obj._id if is_resource(obj) else obj for obj in others}
        return ConditionalPredicate(OP_CODE.EXCLUDING, self, others)

    def fset(self, owner: 'Resource', value):
        field = self.resolver.field
        if value is None:
            super().fset(owner, None)
            return

        processed_value, errors = field.process(value)
        if errors:
            console.error(
                message=f'cannot set {self}',
                data={
                    'resolver': str(self.resolver),
                    'field': field,
                    'schema': owner.ravel.schema,
                    'errors': errors,
                    'value': value,
                }
            )
            raise Exception(str(errors))

        super().fset(owner, processed_value)

    @property
    def asc(self):
        return OrderBy(self.resolver.field.name, desc=False)

    @property
    def desc(self):
        return OrderBy(self.resolver.field.name, desc=True)
