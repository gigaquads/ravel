from typing import Text, Tuple, List, Set, Dict, Type, Union
from collections import defaultdict, deque
from itertools import islice

from appyratus.utils import DictObject
from BTrees.OOBTree import BTree

from ravel.util.misc_functions import (
    get_class_name,
    flatten_sequence,
    union,
)
from ravel.query.predicate import (
    Predicate,
    ConditionalPredicate,
    BooleanPredicate,
)
from ravel.constants import IS_BATCH, OP_CODE
from ravel.schema import Field, String, Int, Bool, Float, Id
from ravel.util.loggers import console
from ravel.util import is_batch, is_resource
from ravel.entity import Entity


class Batch(Entity):

    ravel = DictObject()
    ravel.properties = {}
    ravel.owner = None

    def __init__(self, resources: List = None, indexed=True):
        self.internal = DictObject()
        self.internal.resources = deque(resources or [])
        self.internal.indexed = indexed
        self.internal.indexes = defaultdict(BTree)
        if indexed:
            self.internal.indexes.update({
                k: BTree() for k in self.ravel.properties
            })

    def __len__(self):
        return len(self.internal.resources)

    def __bool__(self):
        return bool(self.internal.resources)

    def __iter__(self):
        return iter(self.internal.resources)

    def __getitem__(self, index):
        if isinstance(index, slice):
            return type(self)(
                islice(
                    self.internal.resources,
                    index.start, index.stop, index.step
                )
            )
        else:
            assert isinstance(index, int)
            return self.internal.resources[index]

    def __setitem__(self, index, resource):
        owner = self.ravel.owner
        if owner and not isinstance(resource, owner):
            raise ValueError(
                f'expected value with type '
                f'{get_class_name(owner)}'
            )
        self.internal.resources[index] = value

    def __repr__(self):
        dirty_count = sum(
            1 for x in self if x and x.internal.state.dirty
        )
        return (
            f'{get_class_name(self.ravel.owner)}.Batch('
            f'size={len(self)}, dirty={dirty_count})'
        )

    def __add__(self, other):
        """
        Create and return a copy, containing the concatenated data lists.
        """
        clone = type(self)(self.internal.resources, indexed=self.indexed)

        if isinstance(other, (list, tuple, set)):
            clone.internal.resources.extend(other)
        elif isinstance(other, Batch):
            assert other.ravel.owner is self.ravel.owner
            clone.internal.resources.extend(other.internal.resources)
        else:
            raise ValueError(str(other))

        return clone

    @classmethod
    def factory(cls, owner: Type['Resource'], type_name=None):
        type_name = type_name or 'Batch'

        # start with inherited ravel object
        ravel = DictObject()

        ravel.owner = owner
        ravel.indexed_field_types = cls.get_indexed_field_types()
        ravel.properties = {}

        for k, resolver in owner.ravel.resolvers.fields.items():
            ravel.properties[k] = BatchResolverProperty(resolver)

        derived_batch_type = type(type_name, (cls, ), dict(
            ravel=ravel, **ravel.properties
        ))

        setattr(derived_batch_type, IS_BATCH, True)

        return derived_batch_type

    @classmethod
    def get_indexed_field_types(cls) -> Tuple['Field']:
        return (String, Bool, Int, Float, Id)

    @classmethod
    def generate(
        cls,
        resolvers: Set[Text] = None,
        values: Dict = None,
        count: int = 1
    ):
        count = max(1, count)
        owner = cls.ravel.owner

        if owner is None:
            # this batch isn't associated with any Resource type
            # and therefore we don't know what to generate.
            raise Exception('unbound Batch type')

        if not resolvers:
            resolvers = set(owner.ravel.resolvers.fields.keys())

        return cls(
            owner.generate(resolvers=resolvers, values=values)
            for i in range(count)
        )

    def merge(self, data=None, **more_data):
        data = dict(data or {}, **more_data)
        for resource in self.internal.resources:
            resource.merge(data)
        return self

    def insert(self, index, resource):
        self.internal.resources.insert(index, resource)
        if self.internal.indexed:
            self._update_indexes(resource)
        return self

    def remove(self, resource):
        try:
            self.internal.resources.remove(resource)
            self._prune_indexes(resource)
        except ValueError:
            raise
        return self

    def append(self, resource):
        self.insert(-1, resource)
        return self

    def appendleft(self, resource):
        self.insert(0, resource)
        return self

    def extend(self, resources):
        self.internal.resources.extend(resources)
        for resource in resources:
            self._update_indexes(resource)

    def extendleft(self, resources):
        self.internal.resources.extendleft(resources)
        for resource in resources:
            self._update_indexes(resource)

    def pop(self, index=-1):
        resource = self.internal.resources.pop(index)
        self._prune_indexes(resource)

    def popleft(self, index=-1):
        resource = self.internal.resources.popleft(index)
        self._prune_indexes(resource)

    def rotate(self, n=1):
        self.internal.resources.rotate(n)
        return self

    def where(self, *predicates, indexed=False):
        predicate = Predicate.reduce_and(flatten_sequence(predicates))
        resources = self._apply_indexes(predicate)
        return type(self)(resources, indexed=indexed)

    def create(self):
        self.ravel.owner.create_many(self.internal.resources)
        return self

    def update(self, state: Dict = None, **more_state):
        self.ravel.owner.update_many(self, data=state, **more_state)
        return self

    def delete(self):
        self.ravel.owner.delete_many({
            resource._id for resource in self.internal.resources
        })
        return self

    def save(self, depth=0):
        self.ravel.owner.save_many(self.internal.resources, depth=depth)
        return self

    def clean(self, resolvers: Set[Text] = None):
        for resource in self.internal.resources:
            # TODO: renamed fields kwarg to resolvers
            resource.clean(fields=resolvers)
        return self

    def mark(self, resolvers: Set[Text] = None):
        for resource in self.internal.resources:
            resource.mark(fields=resolvers)
        return self

    def dump(
        self,
        resolvers: Set[Text] = None,
        style: 'DumpStyle' = None,
    ) -> List[Dict]:
        return [
            resource.dump(resolvers=resolvers, style=style)
            for resource in self.internal.resources
        ]

    def load(self, resolvers: Set[Text] = None):
        stale_id_2_object = {}
        for resource in self.internal.data:
            if resource and resource._id:
                stale_id_2_object[resource._id] = resource

        if stale_id_2_object:
            fresh_objects = self.get_many(
                stale_id_2_object.keys(), select=resolvers
            )
            for fresh_obj in fresh_objects:
                stale_obj = stale_id_2_object.get(fresh_obj._id)
                if stale_obj is not None:
                    stale_obj.merge(fresh_obj)
                    stale_obj.clean(fresh_obj.internal.state.keys())

        return self

    def unload(self, resolvers: Set[Text] = None) -> 'Batch':
        if not resolvers:
            resolvers = set(self.owner.Schema.fields.keys())
        resolvers.discard(ID)
        resolvers.discard(REV)
        for resource in self.internal.resources:
            resource.unload(resolvers)
        return self

    def _update_indexes(self, resource):
        for k, index in self.internal.indexes.items():
            value = resource[k]
            if value not in index:
                index[value] = set()
            index[value].add(resource)

    def _prune_indexes(self, resource):
        for k, index in self.internal.indexes.items():
            value = resource[k]
            if value in index:
                index[value].remove(resource)

    def _apply_indexes(self, predicate):
        if predicate is None:
            return self.records.keys()

        op = predicate.op
        empty = set()
        _ids = set()

        if isinstance(predicate, ConditionalPredicate):
            k = predicate.field.source
            v = predicate.value
            indexes = self.internal.indexes
            index = indexes[k]

            if op == OP_CODE.EQ:
                _ids = indexes[k].get(v, empty)
            elif op == OP_CODE.NEQ:
                _ids = union([
                    _id_set for v_idx, _id_set in index.items()
                    if v_idx != v
                ])
            elif op == OP_CODE.INCLUDING:
                v = v if isinstance(v, set) else set(v)
                _ids = union([index.get(k_idx, empty) for k_idx in v])
            elif op == OP_CODE.EXCLUDING:
                v = v if isinstance(v, set) else set(v)
                _ids = union([
                    _id_set for v_idx, _id_set in index.items()
                    if v_idx not in v
                ])
            else:
                keys = np.array(index.keys(), dtype=object)
                if op == OP_CODE.GEQ:
                    offset = bisect.bisect_left(keys, v)
                    interval = slice(offset, None, 1)
                elif op == OP_CODE.GT:
                    offset = bisect.bisect(keys, v)
                    interval = slice(offset, None, 1)
                elif op == OP_CODE.LT:
                    offset = bisect.bisect_left(keys, v)
                    interval = slice(0, offset, 1)
                elif op == OP_CODE.LEQ:
                    offset = bisect.bisect(keys, v)
                    interval = slice(0, offset, 1)
                else:
                    # XXX: raise StoreError
                    raise Exception('unrecognized op')
                _ids = union([
                    index[k] for k in keys[interval]
                    if k is not None
                ])
        elif isinstance(predicate, BooleanPredicate):
            lhs = predicate.lhs
            rhs = predicate.rhs
            if op == OP_CODE.AND:
                lhs_result = self._apply_indexes(lhs)
                if lhs_result:
                    rhs_result = self._apply_indexes(rhs)
                    _ids = set.intersection(lhs_result, rhs_result)
            elif op == OP_CODE.OR:
                lhs_result = self._apply_indexes(lhs)
                rhs_result = self._apply_indexes(rhs)
                _ids = set.union(lhs_result, rhs_result)
            else:
                # XXX: raise StoreError
                raise Exception('unrecognized boolean predicate')

        return _ids


class BatchResolverProperty(property):
    def __init__(self, resolver):
        self.resolver = resolver
        super().__init__(
            fget=self.fget,
            fset=self.fset,
            fdel=self.fdel,
        )

    def fget(self, batch: 'Batch'):
        key = self.resolver.name
        return [
            getattr(resource, key, None)
            for resource in batch.internal.resources
        ]

    def fset(self, batch: 'Batch', value):
        key = self.resolver.name
        for resource in batch.internal.resources:
            setattr(resource, key, value)

    def fdel(self, batch: 'Batch'):
        key = self.resolver.name
        for resource in batch.internal.resources:
            delattr(resource, key)
