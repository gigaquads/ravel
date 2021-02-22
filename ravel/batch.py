from random import randint
from typing import Text, Tuple, List, Set, Dict, Type, Union, Callable
from collections import defaultdict, deque
from itertools import islice

from appyratus.utils.dict_utils import DictObject
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
from ravel.schema import fields
from ravel.util.loggers import console
from ravel.util import is_batch, is_resource
from ravel.entity import Entity
from ravel.query.order_by import OrderBy


class Batch(Entity):
    """
    A Batch is a collection of Resources. At runtime, Ravel creates a new Batch
    subclass for each Resource type in the app. For example, a User resource
    will have User.Batch. An Account resource will have Account.Batch. Each
    Batch subclass has non-scalar versions of the ResolverProperties owned by
    the corresponding Resource type.

    ## Accessing Resource State in Batch
    Resource and Batch types both implement the same property-based system for
    accessing and writing state, allowing both `user.name` to access the name of
    a single User and `users.name`, where `users` is a User.Batch instance, to
    access a list of all names in the batch. For example:

    ```python
    users = User.Batch([User(name='Augustus'), User(name='Caligula')])
    assert users.name == ['Augustus', 'Caligula']
    ```

    ## CRUD
    Batches implement the same CRUD interface as Resource types, performing the
    batch version of each operation. For example, `users.create()` will call
    `User.create_many(users)` under the hood.

    ## Filtering
    It is possible to filter batches with a "where" method. For this to work,
    a batch must be indexed. Indexing is enabled by default but can be toggled
    through a constructor argument. Filtering is O(log N). The "where" method
    returns a new batch containing the filtered results. This looks like:

    ```python
    filered_users = users.where(User.name == 'Caligula')
    ```

    ## Random Access
    It is possible to access specific resources by indexing a batch like an
    array. For example `users[1:]`. Note that `users["name"]` will not work; for
    that, do `users.name`.
    """

    ravel = DictObject()
    ravel.owner = None
    ravel.properties = {}
    ravel.indexed_field_types = (
        fields.String, fields.Int, fields.Id,
        fields.Bool, fields.Float
    )

    def __init__(self, resources: List = None, indexed=True):
        self.internal = DictObject()
        self.internal.resources = deque(resources or [])
        self.internal.indexed = indexed
        self.internal.indexes = defaultdict(BTree)
        if indexed:
            self.internal.indexes.update({
                k: BTree()
                for k, field in self.ravel.owner.Schema.fields.items()
                if isinstance(field, self.ravel.indexed_field_types)
            })

    def __len__(self):
        return len(self.internal.resources)

    def __bool__(self):
        return bool(self.internal.resources)

    def __iter__(self):
        return iter(self.internal.resources)

    def __getitem__(self, index):
        """
        Return a single resource or a new batch, for the given positional index
        or slice.
        """
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

    def __setitem__(self, index: int, resource: 'Resource'):
        """
        Write a resource to a specified index. The resource being written to the
        batch must belong to the same type as all other elements. That type is
        stored in `Batch.ravel.owner`.
        """
        owner = self.ravel.owner
        if owner and not isinstance(resource, owner):
            raise ValueError(f'wrong Resource type')
        self.internal.resources[index] = value

    def __repr__(self):
        """
        Show the name of the batch, its owner resource type, and its size.
        """
        return (
            f'{get_class_name(self.ravel.owner)}.Batch('
            f'size={len(self)})'
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
        """
        Thie factory method returns new Batch subtypes and is used internally at
        runtime by the Resource metaclass. For example, if you have a Resource
        type called `User`, then this method is invoked to create `User.Batch`.
        """
        type_name = type_name or 'Batch'  # name of new class

        # start with inherited ravel object
        ravel = DictObject()

        ravel.owner = owner
        ravel.indexed_field_types = (
            fields.String, fields.Int, fields.Id,
            fields.Bool, fields.Float
        )
        ravel.properties = {}

        for k, resolver in owner.ravel.resolvers.items():
            ravel.properties[k] = BatchResolverProperty(resolver)

        derived_batch_type = type(type_name, (cls, ), dict(
            ravel=ravel, **ravel.properties
        ))

        # IS_BATCH is a type flag used in is_batch utility methods
        setattr(derived_batch_type, IS_BATCH, True)

        return derived_batch_type

    @classmethod
    def generate(
        cls,
        resolvers: Set[Text] = None,
        values: Dict = None,
        count: int = None,
    ):
        """
        Generate a Batch of fixtures, generating random values for the
        specified resolvers.

        ## Arguments
        - `resolvers`: list of resolver names for which to generate values.
        - `values`: dict of static values to use.
        - `count`: number of resources to generate. defaults between [1, 10].
        """
        owner = cls.ravel.owner

        # if no count, randomize between [1, 10]
        count = max(count, 1) if count is not None else randint(1, 10)

        if owner is None:
            # this batch isn't associated with any Resource type
            # and therefore we don't know what to generate.
            raise Exception('unbound Batch type')
        if not resolvers:
            resolvers = set(owner.ravel.resolvers.fields.keys())
        else:
            resolvers = set(resolvers)

        # create and return the new batch
        return cls(
            owner.generate(resolvers, values=values)
            for _ in range(count)
        )

    def foreach(self, callback: Callable) -> 'Batch':
        for i, x in enumerate(self.internal.resources):
            callback(i, x)
        return self

    def sort(self, order_by) -> 'Batch':
        self.internal.resources = OrderBy.sort(
            self.internal.resources, order_by
        )
        return self

    def set(
        self,
        other: Union[Dict, 'Resource'] = None,
        **values
    ) -> 'Batch':
        return self.merge(other=other, **values)

    def merge(
        self,
        other: Union[Dict, 'Resource'] = None,
        **values
    ) -> 'Batch':
        """
        Batch merge the given dict or resource into all resources contained in
        the batch.
        """
        for resource in self.internal.resources:
            resource.merge(other)
            resource.merge(values)
        return self

    def insert(self, index, resource):
        """
        Insert a resource at the specified index.
        """
        self.internal.resources.insert(index, resource)
        if self.internal.indexed:
            self._update_indexes(resource)
        return self

    def remove(self, resource):
        """
        Remove a resource at the specified index.
        """
        try:
            self.internal.resources.remove(resource)
            self._prune_indexes(resource)
        except ValueError:
            raise
        return self

    def append(self, resource):
        """
        Add a resource at the end of the batch.
        """
        self.insert(-1, resource)
        return self

    def appendleft(self, resource):
        """
        Add a resource at the front of the batch (index: 0)
        """
        self.insert(0, resource)
        return self

    def extend(self, resources):
        """
        Insert a collection of resource at the end of the batch.
        """
        self.internal.resources.extend(resources)
        for resource in resources:
            self._update_indexes(resource)

    def extendleft(self, resources):
        """
        Insert a collection of resource at the front of the batch.
        """
        self.internal.resources.extendleft(resources)
        for resource in resources:
            self._update_indexes(resource)

    def pop(self, index=-1):
        """
        Remove and return the resource at the end of the batch.
        """
        resource = self.internal.resources.pop(index)
        self._prune_indexes(resource)
        return resource

    def popleft(self, index=-1):
        """
        Remove and return the resource at the front of the batch.
        """
        resource = self.internal.resources.popleft(index)
        self._prune_indexes(resource)

    def rotate(self, n=1):
        """
        Shift ever element to the right so that the element at index -1 wraps
        around, back to index 0. The direction can be reversed using a negative
        value of `n`.
        """
        self.internal.resources.rotate(n)
        return self

    def where(self, *predicates: Tuple['Predicate'], indexed=False) -> 'Batch':
        """
        Return a new batch, containing the resources whose attributes match the
        given predicates, like `User.created_at >= cutoff_date`, for instance.
        The new batch is *not* indexed by default, so make sure to set
        `indexed=True` if you intend to filter the filtered batch further.
        """
        predicate = Predicate.reduce_and(flatten_sequence(predicates))
        resources = self._apply_indexes(predicate)
        return type(self)(resources, indexed=indexed)

    def create(self):
        """
        Insert all resources into the store.
        """
        if self:
            self.ravel.owner.create_many(self.internal.resources)
        return self

    def update(self, state: Dict = None, **more_state):
        """
        Apply an update to all resources in the batch, writing it to the store.
        """
        if self:
            self.ravel.owner.update_many(self, data=state, **more_state)
        return self

    def delete(self):
        """
        Delete all resources in the batch fro mthe store.
        """
        if self:
            self.ravel.owner.delete_many(self.internal.resources)
        return self

    def save(self, resolvers: Union[Text, Set[Text]] = None):
        """
        Save all resources in the batch, effectively creating some and updating
        others.
        """
        if not self:
            return self

        # batch save resource resolver targets
        if resolvers is not None:
            owner_resource_type = self.ravel.owner
            fields_to_save = set()

            if isinstance(resolvers, str):
                resolvers = {resolvers}
            elif not isinstance(resolvers, set):
                resolvers = set(resolvers)

            for name in resolvers:
                resolver = owner_resource_type.ravel.resolvers[name]
                if name not in owner_resource_type.ravel.schema.fields:
                    visited_ids = set()
                    unique_targets = resolver.target.Batch()
                    if resolver.many:
                        for batch in getattr(self, name):
                            if batch:
                                unique_targets.extend(batch)
                    else:
                        for resource in getattr(self, name):
                            if resource:
                                unique_targets.append(resource)
                    if unique_targets:
                        unique_targets.save()
                else:
                    fields_to_save.add(name)
        else:
            fields_to_save = None

        # now save fields
        self.ravel.owner.save_many(
            self.internal.resources,
            resolvers=fields_to_save,
        )
        return self

    def clean(self, resolvers: Set[Text] = None):
        """
        Mark all resources in the batch as clean, meaning that no state elements
        are considered dirty and in need up saving to the store.
        """
        for resource in self.internal.resources:
            # TODO: renamed fields kwarg to resolvers
            resource.clean(fields=resolvers)
        return self

    def mark(self, resolvers: Set[Text] = None):
        """
        Mark the given resolvers as dirty, for all resources in the batch.
        """
        for resource in self.internal.resources:
            resource.mark(fields=resolvers)
        return self

    def dump(
        self,
        resolvers: Set[Text] = None,
        style: 'DumpStyle' = None,
    ) -> List[Dict]:
        """
        Return a list with the dump of each resource in the batch.
        """
        return [
            resource.dump(resolvers=resolvers, style=style)
            for resource in self.internal.resources
        ]

    def resolve(self, resolvers: Union[Text, Set[Text]] = None) -> 'Batch':
        """
        Execute each of the resolvers, specified by name, storing the results in
        `self.internal.state`.
        """
        if self._id is None:
            return self

        if isinstance(resolvers, str):
            resolvers = {resolvers}
        elif not resolvers:
            resolvers = set(self.ravel.owner.ravel.schema.fields.keys())

        # execute all requested resolvers
        for k in resolvers:
            resolver = self.ravel.owner.ravel.resolvers.get(k)
            if resolver is not None:
                resolver.resolve_batch(self)

        # clean the resolved values so they arent't accidently saved on
        # update/create, as we just fetched them from the store.
        self.clean(resolvers)

        return self

    def unload(self, resolvers: Set[Text] = None) -> 'Batch':
        """
        Clear resolved state data from all resources in the batch.
        """
        if not resolvers:
            resolvers = set(self.owner.Schema.fields.keys())
        resolvers.discard(ID)
        resolvers.discard(REV)
        for resource in self.internal.resources:
            resource.unload(resolvers)
        return self

    def _update_indexes(self, resource):
        """
        Insert a Resource from the index B-trees.
        """
        for k, index in self.internal.indexes.items():
            if k in resource.internal.state:
                value = resource[k]
                if value not in index:
                    index[value] = set()
                index[value].add(resource)

    def _prune_indexes(self, resource):
        """
        Remove a Resource from the index B-trees.
        """
        for k, index in self.internal.indexes.items():
            if k in resource.internal.state:
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
