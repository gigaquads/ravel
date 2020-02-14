import inspect
import uuid

from typing import Text, Tuple, List, Set, Dict, Type, Union
from pprint import pprint
from collections import defaultdict
from copy import deepcopy

from appyratus.utils import DictObject
from appyratus.memoize import memoized_property
from appyratus.enum import EnumValueStr

import venusian

from pybiz.util.misc_functions import (
    is_sequence,
    get_class_name,
    repr_biz_id,
    flatten_sequence,
    union
)
from pybiz.predicate import Predicate
from pybiz.schema import (
    Field, Schema, String, Int, Id, UuidString, Bool, Float
)
from pybiz.constants import (
    ID_FIELD_NAME,
    REV_FIELD_NAME,
    IS_BIZ_OBJECT_ANNOTATION,
    ABSTRACT_MAGIC_METHOD,
)
from pybiz.store import Store, SimulationStore
from pybiz.util.loggers import console
from pybiz.exceptions import ValidationError
from pybiz.biz.util import is_batch, is_resource
from pybiz.biz.query.order_by import OrderBy
from pybiz.biz.dumper import Dumper, NestedDumper, SideLoadedDumper, DumpStyle
from pybiz.biz.dirty import DirtyDict
from pybiz.biz.entity import Entity


# for batch.py
from collections import defaultdict, deque
from BTrees.OOBTree import BTree
from pybiz.schema import (
    Field, Schema, String, Int, Id, UuidString, Bool, Float
)

class Batch(Entity):

    def __init__(self, resources: List = None, indexed=True):
        self.internal = DictObject()
        self.internal.resources = deque(resources or [])
        self.internal.indexed = indexed
        self.internal.indexes = defaultdict(BTree)
        if indexed:
            self.internal.indexes.update({
                k: BTree() for k in self.pybiz.resolver_properties
            })

    def __len__(self):
        return len(self.internal.resources)

    def __bool__(self):
        return bool(self.internal.resources)

    def __iter__(self):
        return iter(self.internal.resources)

    def __repr__(self):
        dirty_count = sum(
            1 for x in self if x and x.internal.state.dirty
        )
        return (
            f'{get_class_name(self.pybiz.owner)}.Batch('
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
            assert other.pybiz.owner is self.pybiz.owner
            clone.internal.resources.extend(other.internal.resources)
        else:
            raise ValueError(str(other))

        return clone

    @classmethod
    def factory(cls, owner: Type['Resource'], type_name=None):
        type_name = type_name or 'Batch'

        pybiz = DictObject()
        pybiz.owner = owner
        pybiz.indexed_field_types = cls.get_indexed_field_types()
        pybiz.resolver_properties = {
            k: BatchResolverProperty(resolver)
            for k, resolver in owner.pybiz.resolvers.fields.items()
            if isinstance(resolver.field, pybiz.indexed_field_types)
        }

        return type(type_name, (cls, ), dict(
            pybiz=pybiz, **pybiz.resolver_properties
        ))

    @classmethod
    def get_indexed_field_types(cls) -> Tuple['Field']:
        return (String, Bool, Int, Float)

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
        self.pybiz.owner.create_many(self.internal.resources)
        return self

    def update(self, state: Dict = None, **more_state):
        self.pybiz.owner.update_many(self, data=state, **more_state)
        return self

    def delete(self):
        self.pybiz.owner.delete_many({
            resource._id for resource in self.internal.resources
        })
        return self

    def save(self, depth=0):
        self.pybiz.owner.save_many(self.internal.resources, depth=depth)
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
        resolvers.discard(ID_FIELD_NAME)
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

    def fget(self, batch: 'Batch', resolver: Text):
        key = self.resolver.name
        return [
            getattr(resource, key, None)
            for resource in batch.internal.resources
        ]

    def fset(self, batch: 'Batch', value):
        key = self.resolver.name
        for resource in self.internal.resources:
            setattr(resource, key, value)

    def fdel(self, batch: 'Batch'):
        key = self.resolver.name
        for resource in self.internal.resources:
            delattr(resource, key)



class Executor(object):
    def execute(self, query):
        resources = self._fetch_resources(query)

        self._execute_resolvers(query, resources)

        retval = resources
        if query.options.first:
            retval = resources[0] if resources else None

        return retval

    def _fetch_resources(self, query):
        store = query.target.pybiz.store
        where_predicate = query.parameters.where
        field_names = [req.resolver.field.name for req in query.selected.fields]
        state = store.query(predicate=where_predicate, fields=field_names)
        return [query.target(s).clean() for s in state]

    def _execute_resolvers(self, query, resources):
        for request in query.selected.requests:
            resolver = request.resolver
            for resource in resources:
                value = resolver.resolve(resource, request)
                setattr(resource, resolver.name, value)


class ResolverProperty(property):
    def __init__(self, resolver):
        self.resolver = resolver
        super().__init__(
            fget=self.fget,
            fset=self.fset,
            fdel=self.fdel,
        )

    def select(self, *selectors):
        selectors = flatten_sequence(selectors)
        return Request(self.resolver, selectors=selectors)

    def fget(self, owner):
        resolver = self.resolver
        if resolver.name not in owner.internal.state:
            request = Request(resolver)
            owner.internal.state[resolver.name] = resolver.resolve(owner, request)
        return owner.internal.state.get(resolver.name)

    def fset(self, owner, value):
        resolver = self.resolver
        owner.internal.state[resolver.name] = value

    def fdel(self, owner):
        resolver = self.resolver
        owner.internal.state.pop(resolver.name)


# for resolver.py
import sys

from typing import Text, Set, Dict, List, Callable, Type, Tuple

from pybiz.util.loggers import console
from pybiz.util.misc_functions import get_class_name
from pybiz.biz.util import is_resource, is_batch


class Resolver(object):

    app = None

    def __init__(
        self,
        name=None,
        owner=None,
        target=None,
        on_resolve=None,
        private=False,
        required=False,
    ):
        self.name = name
        self.on_resolve = on_resolve or self.on_resolve
        self.owner = owner
        self.private = private
        self.required = required
        self.target_callback = None
        self.target = None
        self.many = None

        if is_resource(target):
            assert isinstance(target, type)
            self.target = target
            self.many = False
        elif is_batch(target):
            self.target = target.owner
            self.many = True
        elif target is not None:
            assert callable(target)
            self.target_callback = target

        self._is_bootstrapped = False

    def resolve(self, resource, request):
        self.pre_resolve(resource, request)
        result = self.on_resolve(resource, request)
        processed_result = self.post_resolve(resource, request, result)
        return processed_result

    def on_bind(self):
        pass

    @classmethod
    def on_bootstrap(cls):
        pass

    @property
    def is_bootstrapped(self):
        return self._is_bootstrapped

    @classmethod
    def property_type(cls):
        return ResolverProperty

    @classmethod
    def build_property(cls, *args, **kwargs):
        resolver = cls(*args, **kwargs)
        property_type = cls.property_type()
        return property_type(resolver)

    @classmethod
    def priority(self) -> int:
        """
        Proprity defines the order in which Resolvers execute when executing a
        query, in ascending order.
        """
        return sys.maxsize

    @classmethod
    def tags(cls) -> Set[Text]:
        """
        In development, you can access all Resolvers by tag.  For example, if
        you had a User class with a Resolver called "account" whose class was
        tagged with "my_tag", then you could access this resolver in a
        tag-specific dictionary by doing this:

        ```py
        account_resolver = User.resolvers.my_tag['account']

        # equivalent to doing...
        account_resolver = User.account.resolver
        ```
        """
        return {'untagged'}

    @staticmethod
    def sort(resolvers: List['Resolver']) -> List['Resolver']:
        """
        Sort and return the input resolvers as a new list, orderd by priority
        int, ascending. This reflects the relative order of intended execution,
        from first to last.
        """
        return sorted(resolvers, key=lambda resolver: resolver.priority())

    def bootstrap(cls, app: 'Application'):
        cls.app = app
        cls.on_bootstrap()
        cls._is_bootstrapped = True

    def bind(self):
        if self.target_callback:
            self.app.inject(self.target_callback)
            target = self.target_callback()
            if is_resource(target):
                assert isinstance(target, type)
                self.target = target
                self.many = False
            elif is_batch(target):
                self.target = target.owner
                self.many = True

        self.on_bind()
        self._is_bound = True

    @staticmethod
    def pre_resolve(resource, request):
        return

    @staticmethod
    def on_resolve(resource, request):
        raise NotImplementedError()

    @staticmethod
    def post_resolve(resource, request, result):
        return result


class EagerStoreLoader(Resolver):
    def __init__(self, field, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.field = field

    @classmethod
    def property_type(cls):
        return EagerStoreLoaderProperty

    @classmethod
    def tags(cls) -> Set[Text]:
        return {'fields'}

    @classmethod
    def priority(cls) -> int:
        return 1

    @staticmethod
    def on_resolve(resource, request):
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


from pybiz.biz.query.order_by import OrderBy
from pybiz.predicate import (
    Predicate,
    ConditionalPredicate,
    BooleanPredicate,
    ResolverAlias,
    OP_CODE,
)

class EagerStoreLoaderProperty(ResolverProperty):
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



class ResolverDecorator(object):
    def __init__(
            self, target=None, resolver=None, on_resolve=None,
            *args, **kwargs
        ):
        self.resolver_type = resolver or Resolver
        self.target = target
        self.on_resolve = on_resolve
        self.args = args
        self.kwargs = kwargs

    def __call__(self, on_resolve):
        self.on_resolve = on_resolve
        return self

    def build_resolver_property(self, owner, name):
        resolver_property = self.resolver_type.build_property(
            name=name,
            owner=owner,
            target=self.target,
            on_resolve=self.on_resolve,
            *self.args,
            **self.kwargs
        )
        return resolver_property


class relationship(ResolverDecorator):
    def __init__(self, *args, **kwargs):
        super().__init__(resolver=Relationship, *args, **kwargs)


class Relationship(Resolver):

    class Join(object):
        def __init__(self, left, right):
            self.left = left
            self.right = right

        def build_query(self, source):
            query = self.right.resolver.owner.select()

            if is_resource(source):
                source_value = getattr(source, self.left.resolver.field.name)
                query.where(self.right == source_value)
            else:
                assert is_batch(source)
                source_values = getattr(source, self.left.resolver.field.name)
                query.where(self.right.including(source_values))

            return query

    def __init__(self, join, *args, **kwargs):
        if callable(join):
            self.join_callback = join
            self.joins = []
        else:
            self.join_callback = None
            self.joins = join

        super().__init__(*args, **kwargs)

    @classmethod
    def tags(cls) -> Set[Text]:
        return {'relationships'}

    @classmethod
    def priority(cls) -> int:
        return 10

    def on_bind(self):
        if self.join_callback is not None:
            self.app.inject(self.join_callback)
        self.joins = [self.Join(l, r) for l, r in self.join_callback()]
        self.target = self.joins[-1].right.resolver.owner

    @staticmethod
    def pre_resolve(resource, request):
        # TODO: build, execute query, set on request.result
        rel = request.resolver
        source = resource
        joins = rel.joins
        final_join = joins[-1]

        results = []

        if len(joins) == 1:
            query = final_join.build_query(source)
            query.select(final_join.right.resolver.owner.pybiz.resolvers.fields)
            result = query.execute(first=not rel.many)
            results.append(result)
        else:
            for j1, j2 in zip(joins, join[1:]):
                query = j1.build_query(source)
                query.select(j2.left.resolver.field.name)
                if j2 is final_join:
                    results.append(query.execute(first=not rel.many))
                else:
                    results.append(query.execute())

        request.result = results[-1]


# for resolver_manager.py
from typing import Text, Set, Dict, List, Callable, Type, Tuple
from collections import defaultdict

from pybiz.util.loggers import console
from pybiz.biz.util import is_resource, is_batch


class ResolverManager(object):
    @classmethod
    def copy(cls, manager):
        copy = cls()
        copy._resolvers = manager._resolvers.copy()
        copy._tag_2_resolvers = manager._tag_2_resolvers.copy()
        copy._required_resolvers = manager._required_resolvers.copy()
        copy._private_resolvers = manager._private_resolvers.copy()
        return copy

    def __init__(self):
        self._resolvers = {}
        self._tag_2_resolvers = defaultdict(dict)
        self._required_resolvers = set()
        self._private_resolvers = set()

    def __getattr__(self, tag):
        return self.by_tag(tag)

    def __getitem__(self, name):
        return self._resolvers.get(name)

    def __setitem__(self, name, resolver):
        assert name == resolver.name
        self[name] = resolver

    def __iter__(self):
        return iter(self._resolvers)

    def __contains__(self, obj):
        if isinstance(obj, Resolver):
            return obj.name in self._resolvers
        else:
            return obj in self._resolvers

    def __len__(self):
        return len(self._resolvers)

    def get(self, key, default=None):
        return self._resolvers.get(key, default)

    def keys(self):
        return list(self._resolvers.keys())

    def values(self):
        return list(self._resolvers.values())

    def items(self):
        return list(self._resolvers.items())

    @property
    def required_resolvers(self) -> Set[Resolver]:
        return self._required_resolvers

    @property
    def private_resolvers(self) -> Set[Resolver]:
        return self._private_resolvers

    def register(self, resolver):
        name = resolver.name
        old_resolver = self._resolvers.get(name)
        if old_resolver is not None:
            del self._resolvers[name]
            if old_resolver.required:
                self._required_resolvers.remove(old_resolver)
            if old_resolver.private:
                self._private_resolvers.remove(old_resolver)
            for tag in old_resolver.tags():
                del self._tag_2_resolvers[tag][name]

        self._resolvers[name] = resolver

        if resolver.required:
            self._required_resolvers.add(resolver)
        if resolver.private:
            self._private_resolvers.add(resolver)

        for tag in resolver.tags():
            self._tag_2_resolvers[tag][name] = resolver

    def by_tag(self, tag, invert=False):
        if not invert:
            return self._tag_2_resolvers.get(tag, {})
        else:
            resolvers = {}
            keys_to_exclude = self._tag_2_resolvers.get(tag, {}).keys()
            for tag_key, resolver_dict in self._tag_2_resolvers.items():
                if tag_key != tag:
                    resolvers.update(resolver_dict)
            return resolvers


class Request(object):
    def __init__(self, resolver):
        self.resolver = resolver
        self.parameters = DictObject()
        self.result = None

    def __repr__(self):
        return (
            f'Request('
            f'{get_class_name(self.resolver.owner)}.'
            f'{self.resolver.name}'
            f')'
        )

    def __getattr__(self, name):
        return ParameterAssignment(name, self)

    @memoized_property
    def query(self):
        return Query(
            target=self.resolver.target,
        ).select(
            self.parameters.select
        ).where(
            self.parameters.where
        ).order_by(
            self.parameters.order_by
        ).offset(
            self.parameters.offset
        ).limit(
            self.parameters.limit
        )


class ParameterAssignment(object):
    """
    This is an internal data structure, used to facilitate the syntactic sugar
    that allows you to write to query.params via funcion call notation, like
    query.foo('bar') instead of query.params['foo'] = bar.

    Instances of this class just store the query whose parameter we are going to
    set and the name of the dict key or "param name". When called, it writes the
    single argument supplied in the call to the params dict of the query.
    """

    def __init__(self, name, query=None, parameters=None):
        self._params = parameters
        self._name = name

    def __call__(self, param):
        """
        Store the `param` value in the Query's parameters dict.
        """
        self._params[self._name] = param
        return self._query

    def __repr__(self):
        return (
            f'{get_class_name(self)}'
            f'('
            f'parameter={self._name}'
            f')'
        )




class Query(object):

    executor = Executor()

    def __init__(self, target=None, parent=None, parameters=None, options=None):
        self.target = target
        self.parent = parent
        self.options = options or DictObject()
        self.parameters = parameters or DictObject()
        self.selected = DictObject()
        self.selected.fields = {}
        self.selected.requests = {}

    def __getattr__(self, parameter_name: str):
        return ParameterAssignment(parameter_name, self)

    def execute(self, first=None):
        if first is not None:
            self.options.first = first
        return self.executor.execute(self)

    def merge(self, other: 'Query', in_place=False) -> 'Query':
        if in_place:
            self.parameters.update(deepcopy(other.parameters))
            self.options.update(deepcopy(other.options))
            self.selected.fields.update(deepcopy(other.selected.fields))
            self.selected.requests.update(deepcopy(other.selected.requests))
            return self
        else:
            merged_query = type(self)(
                target=other.target or self.target,
                parent=other.parent or self.parent,
            )
            merged_query.merge(self, in_place=True)
            merged_query.merge(other, in_place=True)
            return merged_query


    def select(self, *selectors):
        selectors = flatten_sequence(selectors)

        for obj in selectors:
            if isinstance(obj, str):
                # if obj is str, replace it with the corresponding resolver
                # property from the target Resource class.
                _obj = getattr(self.target, obj, None)
                if _obj is None:
                    raise ValueError(f'unknown resolver: {obj}')
                obj = _obj

            # insert a Request object into self.selected
            if isinstance(obj, EagerStoreLoaderProperty):
                resolver_property = obj
                request = Request(resolver_property.resolver)
                self.selected.fields[request.resolver.name] = request
            elif isinstance(obj, ResolverProperty):
                resolver_property = obj
                request = Request(resolver_property.resolver)
                self.selected.requests[request.resolver.name] = request
            elif isinstance(obj, Request):
                request = obj
                if isinstance(request.resolver, EagerStoreLoader):
                    self.selected.fields[request.resolver.name] = request
                else:
                    self.selected.requests[request.resolver.name] = request

        return self

    def where(self, *predicates, append=True):
        predicates = flatten_sequence(predicates)
        if predicates:
            if self.parameters.where:
                self.parameters.where &= Predicate.reduce_and(predicates)
            else:
                self.parameters.where = Predicate.reduce_and(predicates)
        elif not append:
            self.parameters.where = None
        return self

    def order_by(self, *order_by):
        order_by = flatten_sequence(order_by)

        if order_by:
            self.parameters.order_by = []

            for obj in order_by:
                if isinstance(obj, OrderBy):
                    self.parameters.order_by.append(obj)
                elif isinstance(obj, str):
                    if obj.lower().endswith(' desc'):
                        order_by_obj = OrderBy(obj.split()[0], desc=True)
                    else:
                        order_by_obj = OrderBy(obj.split()[0], desc=False)
                    if order_by_obj.key not in self.target.pybiz.resolvers:
                        raise ValueError(
                            f'uncognized resolver: {order_by_obj.key}'
                        )
                    self.parameters.order_by.append(order_by_obj)
        else:
            self.parameters.order_by = None

        return self

    def offset(self, offset=None):
        if offset is not None:
            self.parameters.offset = max(0, int(offset))
        else:
            self.parameters.offset = None
        return self

    def limit(self, limit):
        if limit is not None:
            self.parameters.limit = max(1, int(limit))
        else:
            self.parameters.limit = None
        return self


class ResourceMeta(type):
    def __init__(cls, name, bases, dct):
        cls._initialize_class_state()

        fields = cls._process_fields()

        cls._build_schema_class(fields, bases)
        cls.Batch = Batch.factory(cls)

        if not cls.pybiz.is_abstract:
            cls._register_venusian_callback()

    def _initialize_class_state(biz_class):
        setattr(biz_class, IS_BIZ_OBJECT_ANNOTATION, True)

        biz_class.pybiz = DictObject()
        biz_class.pybiz.app = None
        biz_class.pybiz.store = None
        biz_class.pybiz.resolvers = ResolverManager()
        biz_class.pybiz.fk_id_fields = {}
        biz_class.pybiz.is_abstract = biz_class._compute_is_abstract()
        biz_class.pybiz.is_bootstrapped = False
        biz_class.pybiz.is_bound = False
        biz_class.pybiz.schema = None
        biz_class.pybiz.defaults = {}

    def _register_venusian_callback(biz_class):
        def callback(scanner, name, biz_class):
            """
            Callback used by Venusian for Resource class auto-discovery.
            """
            console.info(f'venusian scan found "{biz_class.__name__}" Resource')
            scanner.biz_classes.setdefault(name, biz_class)

        venusian.attach(biz_class, callback, category='biz')

    def _process_fields(cls):
        fields = {}
        for k, v in inspect.getmembers(cls):
            if isinstance(v, ResolverDecorator):
                resolver_property = v.build_resolver_property(owner=cls, name=k)
                cls.pybiz.resolvers.register(resolver_property.resolver)
                setattr(cls, k, resolver_property)
            if isinstance(v, Field):
                field = v
                field.name = k
                fields[k] = field
                resolver_property = EagerStoreLoader.build_property(
                    owner=cls, field=field, name=k, target=cls,
                )
                cls.pybiz.resolvers.register(resolver_property.resolver)
                setattr(cls, k, resolver_property)
        return fields

    def _compute_is_abstract(biz_class):
        is_abstract = False
        if hasattr(biz_class, ABSTRACT_MAGIC_METHOD):
            is_abstract = bool(biz_class.__abstract__())
            delattr(biz_class, ABSTRACT_MAGIC_METHOD)
        return is_abstract

    def _build_schema_class(biz_class, fields, base_classes):
        fields = fields.copy()
        inherited_fields = {}

        # inherit fields and defaults from base Resource classes
        for base_class in base_classes:
            if is_resource(base_class):
                inherited_fields.update(base_class.Schema.fields)
                biz_class.pybiz.defaults.update(base_class.pybiz.defaults)
            else:
                base_fields = biz_class._copy_fields_from_mixin(base_class)
                inherited_fields.update(base_fields)

        fields.update(inherited_fields)

        # perform final processing now that we have all direct and
        # inherited fields in one dict.
        for k, field in fields.items():
            if k in inherited_fields:
                resolver_property = EagerStoreLoader.build_property(
                    owner=biz_class, field=field, name=k, target=biz_class,
                )
                biz_class.pybiz.resolvers.register(resolver_property.resolver)
                setattr(biz_class, k, resolver_property)
            if field.source is None:
                field.source = field.name
            if isinstance(field, Id) and field.name != ID_FIELD_NAME:
                    biz_class.pybiz.fk_id_fields[field.name] = field

        # these are universally required
        assert ID_FIELD_NAME in fields
        assert REV_FIELD_NAME in fields

        # build new Schema subclass with aggregated fields
        class_name = f'{biz_class.__name__}Schema'
        biz_class.Schema = type(class_name, (Schema, ), fields)

        biz_class.pybiz.schema = schema = biz_class.Schema()
        biz_class.pybiz.defaults = biz_class._extract_field_defaults(schema)

    def _copy_fields_from_mixin(biz_class, class_obj):
        fields = {}
        is_field = lambda x: isinstance(x, Field)
        for k, field in inspect.getmembers(class_obj, predicate=is_field):
            if k == 'Schema':
                continue
            fields[k] = deepcopy(field)
        return fields

    def _extract_field_defaults(biz_class, schema):
        defaults = biz_class.pybiz.defaults
        for field in schema.fields.values():
            if field.default:
                # move field default into "defaults" dict
                if callable(field.default):
                    defaults[field.name] = field.default
                else:
                    defaults[field.name] = lambda: deepcopy(field.default)
                # clear it from the schema object once "defaults" dict
                field.default = None
        return defaults


class Resource(Entity, metaclass=ResourceMeta):

    _id = UuidString(default=lambda: uuid.uuid4().hex)
    _rev = String()

    def __init__(self, state=None, **more_state):
        # initialize internal state data dict
        self.internal = DictObject()
        self.internal.state = DirtyDict()
        self.merge(state, **more_state)

        # eagerly generate default ID if none provided
        if ID_FIELD_NAME not in self.internal.state:
            id_func = self.pybiz.defaults.get(ID_FIELD_NAME)
            self.internal.state[ID_FIELD_NAME] = id_func() if id_func else None

    def __getitem__(self, key):
        if key in self.pybiz.resolvers:
            return getattr(self, key)
        raise KeyError(key)

    def __setitem__(self, key, value):
        if key in self.pybiz.resolvers:
            return setattr(self, key, value)
        raise KeyError(key)

    def __delitem__(self, key):
        if key in self.pybiz.resolvers:
            delattr(self, key)
        else:
            raise KeyError(key)

    def __iter__(self):
        return iter(self.internal.state)

    def __contains__(self, key):
        return key in self.internal.state

    def __repr__(self):
        id_str = repr_biz_id(self)
        name = get_class_name(self)
        dirty = '*' if self.internal.state.dirty else ''
        return f'<{name}({id_str}){dirty}>'

    @classmethod
    def __abstract__(cls) -> bool:
        return True

    @classmethod
    def __store__(cls) -> Type[Store]:
        return SimulationStore

    @classmethod
    def on_bootstrap(cls, app, *args, **kwargs):
        pass

    @classmethod
    def on_bind(cls):
        pass

    @classmethod
    def bootstrap(cls, app, *args, **kwargs):
        cls.pybiz.app = app

        # resolve the concrete Field class to use for each "foreign key"
        # ID field referenced by this class.
        for id_field in cls.pybiz.fk_id_fields.values():
            id_field.replace_self_in_biz_class(app, cls)

        # bootstrap all resolvers owned by this class
        for resolver in cls.pybiz.resolvers.values():
            resolver.bootstrap(app)

        # lastly perform custom developer logic
        cls.on_bootstrap(app, *args, **kwargs)
        cls.pybiz.is_bootstrapped = True

    @classmethod
    def bind(cls, binder: 'ResourceBinder', **kwargs):
        cls.pybiz.store = cls.pybiz.app.binder.get_binding(cls).store_instance
        for resolver in cls.pybiz.resolvers.values():
            resolver.bind()
        cls.on_bind()
        cls.pybiz.is_bound = True

    @classmethod
    def is_bootstrapped(cls) -> bool:
        return cls.pybiz.is_bootstrapped

    @classmethod
    def is_bound(cls) -> bool:
        return cls.pybiz.is_bound

    @property
    def store(self) -> 'Store':
        return self.pybiz.store

    @property
    def dirty(self) -> Set[Text]:
        return {
            k: self.internal.state[k]
            for k in self.internal.state.dirty
            if k in self.Schema.fields
        }

    @classmethod
    def generate(cls, query: 'Query' = None) -> 'Resource':
        instance = cls()
        query = query or cls.select(cls.pybiz.resolvers.fields.keys())
        resolvers = Resolver.sort(
            cls.pybiz.resolvers[k] for k in query.selected.fields
        )
        for resolver in resolvers:
            if resolver.name == REV_FIELD_NAME:
                setattr(instance, resolver.name, '0')
            else:
                request = query.parameters.selected.fields[resolver.name]
                generated_value = resolver.generate(instance, request)
                setattr(instance, resolver.name, generated_value)
        return instance

    def merge(self, other=None, **values) -> 'Resource':
        if isinstance(other, dict):
            for k, v in other.items():
                setattr(self, k, v)
        elif isinstance(other, Resource):
            for k, v in other.internal.state.items():
                setattr(self, k, v)

        if values:
            self.merge(values)

        return self

    def clean(self, fields=None) -> 'Resource':
        if fields:
            fields = fields if is_sequence(fields) else {fields}
            keys = self._normalize_selectors(fields)
        else:
            keys = set(self.pybiz.resolvers.keys())

        if keys:
            self.internal.state.clean(keys=keys)

        return self

    def mark(self, fields=None) -> 'Resource':
        # TODO: rename "mark" method to "touch"
        if fields is not None:
            if not fields:
                return self
            fields = fields if is_sequence(fields) else {fields}
            keys = self._normalize_selectors(fields)
        else:
            keys = set(self.Schema.fields.keys())

        self.internal.state.mark(keys)
        return self

    def dump(self, resolvers: Set[Text] = None, style: DumpStyle = None) -> Dict:
        """
        Dump the fields of this business object along with its related objects
        (declared as relationships) to a plain ol' dict.
        """
        # get Dumper instance based on DumpStyle (nested, side-loaded, etc)
        dumper = Dumper.for_style(style or DumpStyle.nested)

        if resolvers is not None:
            # only dump resolver state specifically requested
            keys_to_dump = self._normalize_selectors(resolvers)
        else:
            # or else dump all instance state
            keys_to_dump = list(self.internal.state.keys())

        dumped_instance_state = dumper.dump(self, keys=keys_to_dump)
        return dumped_instance_state

    def copy(self) -> 'Resource':
        """
        Create a clone of this Resource
        """
        clone = type(self)(data=deepcopy(self.internal.state))
        return clone.clean()

    def load(self, resolvers: Set[Text] = None) -> 'Resource':
        if self._id is None:
            return self

        if isinstance(resolvers, str):
            resolvers = {resolvers}

        # TODO: fix up Query so that even if the fresh object does exist in the
        # DAL, it will still try to execute the resolvers on the uncreated
        # object.

        # resolve a fresh copy throught the DAL and merge state
        # into this Resource.
        query = self.select(resolvers).where(_id=self._id)
        fresh = query.execute(first=True)
        if fresh:
            self.merge(fresh)
            self.clean(fresh.internal.state.keys())

        return self

    def reload(self, resolvers: Set[Text] = None) -> 'Resource':
        if isinstance(resolvers, str):
            resolvers = {resolvers}
        loading_resolvers = {k for k in resolvers if self.is_loaded(k)}
        return self.load(loading_resolvers)

    def unload(self, resolvers: Set[Text] = None) -> 'Resource':
        """
        Remove the given keys from field data and/or relationship data.
        """
        if resolvers:
            if isinstance(resolvers, str):
                resolvers = {resolvers}
                keys = self._normalize_selectors(resolvers)
        else:
            keys = set(
                self.internal.state.keys() |
                self.pybiz.resolvers.keys()
            )
        for k in keys:
            if k in self.internal.state:
                del self.internal.state[k]
            elif k in self.pybiz.resolvers:
                del self.pybiz.resolvers[k]

    def is_loaded(self, resolvers: Union[Text, Set[Text]]) -> bool:
        """
        Are all given field and/or relationship values loaded?
        """
        if resolvers:
            if isinstance(resolvers, str):
                resolvers = {resolvers}
                keys = self._normalize_selectors(resolvers)
        else:
            keys = set(
                self.internal.state.keys() |
                self.pybiz.resolvers.keys()
            )

        for k in keys:
            is_key_in_data = k in self.internal.state
            is_key_in_resolvers = k in self.pybiz.resolvers
            if not (is_key_in_data or is_key_in_resolvers):
                return False

        return True

    def _prepare_record_for_create(self):
        """
        Prepares a a Resource state dict for insertion via DAL.
        """
        # extract only those elements of state data that correspond to
        # Fields declared on this Resource class.
        record = {
            k: v for k, v in self.internal.state.items()
            if k in self.pybiz.resolvers.fields
        }
        # when inserting or updating, we don't want to write the _rev value on
        # accident. The DAL is solely responsible for modifying this value.
        if REV_FIELD_NAME in record:
            del record[REV_FIELD_NAME]

        # generate default values for any missing fields
        # that specifify a default
        for k, default in self.pybiz.defaults.items():
            if k not in record:
                def_val = default()
                record[k] = def_val

        if record.get(ID_FIELD_NAME) is None:
            record[ID_FIELD_NAME] = self.store.create_id(record)

        return record

    @staticmethod
    def _normalize_selectors(selectors: Set):
        keys = set()
        for k in selectors:
            if isinstance(k, str):
                keys.add(k)
            elif isinstance(k, ResolverProperty):
                keys.add(k.name)
        return keys

    # CRUD Methods

    @classmethod
    def select(cls, *resolvers: Tuple[Text], parent: 'Query' = None):
        return Query(target=cls, parent=parent).select(resolvers)

    def create(self, data: Dict = None) -> 'Resource':
        if data:
            self.merge(data)

        prepared_record = self._prepare_record_for_create()
        prepared_record.pop(REV_FIELD_NAME, None)

        created_record = self.store.dispatch('create', (prepared_record, ))

        self.internal.state.update(created_record)
        return self.clean()

    @classmethod
    def get(cls, _id, select=None) -> 'Resource':
        if _id is None:
            return None
        if not select:
            data = cls.get_store().fetch(_id)
            return cls(data=data).clean() if data else None
        else:
            return cls.query(
                select=select,
                where=(cls._id == _id),
                first=True
            )

    @classmethod
    def get_many(
        cls,
        _ids: List = None,
        select=None,
        offset=None,
        limit=None,
        order_by=None,
    ) -> 'Batch':
        """
        Return a list of Resources in the store.
        """
        if not _ids:
            return cls.Batch()
        if not (select or offset or limit or order_by):
            store = cls.get_store()
            id_2_data = store.dispatch('fetch_many', (_ids, ))
            return cls.Batch(cls(data=data) for data in id_2_data.values())
        else:
            return cls.query(
                select=select,
                where=cls._id.including(_ids),
                order_by=order_by,
                offset=offset,
                limit=limit,
            )

    @classmethod
    def get_all(
        cls,
        select: Set[Text] = None,
        offset: int = None,
        limit: int = None,
    ) -> 'Batch':
        """
        Return a list of all Resources in the store.
        """
        return cls.query(
            select=select,
            where=cls._id != None,
            order_by=cls._id.asc,
            offset=offset,
            limit=limit,
        )

    def delete(self) -> 'Resource':
        """
        Call delete on this object's store and therefore mark all fields as dirty
        and delete its _id so that save now triggers Store.create.
        """
        self.store.dispatch('delete', (self._id, ))
        self.mark(self.internal.state.keys())
        self._id = None
        return self

    @classmethod
    def delete_many(cls, resources) -> None:
        # extract ID's of all objects to delete and clear
        # them from the instance objects' state dicts
        resource_ids = []
        for obj in resources:
            obj.mark(obj.internal.state.keys())
            resource_ids.append(obj._id)
            obj._id = None

        # delete the records in the DAL
        store = cls.get_store()
        store.dispatch('delete_many', args=(resource_ids, ))

    @classmethod
    def delete_all(cls) -> None:
        store = cls.get_store()
        store.dispatch('delete_all')

    def exists(self) -> bool:
        """
        Does a simple check if a Resource exists by id.
        """
        if self._id is not None:
            return self.store.dispatch('exists', args=(self._id, ))
        return False

    def save(self, depth=0):
        return self.save_many([self], depth=depth)[0]

    def create(self, data: Dict = None) -> 'Resource':
        if data:
            self.merge(data)

        prepared_record = self._prepare_record_for_create()
        prepared_record.pop(REV_FIELD_NAME, None)

        created_record = self.store.dispatch('create', (prepared_record, ))

        self.internal.state.update(created_record)
        return self.clean()

    def update(self, data: Dict = None, **more_data) -> 'Resource':
        data = dict(data or {}, **more_data)
        if data:
            self.merge(data)

        raw_record = self.dirty.copy()
        raw_record.pop(REV_FIELD_NAME, None)
        raw_record.pop(ID_FIELD_NAME, None)

        errors = {}
        prepared_record = {}
        for k, v in raw_record.items():
            field = self.Schema.fields.get(k)
            if field is not None:
                prepared_record[k], error = field.process(v)
                if error:
                    errors[k] = error

        if errors:
            raise ValidationError(
                message=f'could not update {get_class_name(self)} object',
                data={
                    ID_FIELD_NAME: self._id,
                    'errors': errors,
                }
            )

        updated_record = self.store.dispatch(
            'update', (self._id, prepared_record)
        )

        self.internal.state.update(updated_record)
        return self.clean()

    @classmethod
    def create_many(cls, resources: List['Resource']) -> 'Batch':
        """
        Call `store.create_method` on input `Resource` list and return them in
        the form of a Batch.
        """
        records = []

        for resource in resources:
            if resource is None:
                continue
            if isinstance(resource, dict):
                resource = cls(data=resource)

            record = resource._prepare_record_for_create()
            records.append(record)

        store = cls.get_store()
        created_records = store.dispatch('create_many', (records, ))

        for resource, record in zip(resources, created_records):
            resource.internal.state.update(record)
            resource.clean()

        return cls.Batch(resources)

    @classmethod
    def update_many(
        cls, resources: List['Resource'], data: Dict = None, **more_data
    ) -> 'Batch':
        """
        Call the Store's update_many method on the list of Resources. Multiple
        Store calls may be made. As a preprocessing step, the input resource list
        is partitioned into groups, according to which subset of fields are
        dirty.

        For example, consider this list of resources,

        ```python
        resources = [
            user1,     # dirty == {'email'}
            user2,     # dirty == {'email', 'name'}
            user3,     # dirty == {'email'}
        ]
        ```

        Calling update on this list will result in two paritions:
        ```python
        assert part1 == {user1, user3}
        assert part2 == {user2}
        ```

        A spearate DAO call to `update_many` will be made for each partition.
        """
        # common_values are values that should be updated
        # across all objects.
        common_values = dict(data or {}, **more_data)

        # in the procedure below, we partition all incoming Resources
        # into groups, grouped by the set of fields being updated. In this way,
        # we issue an update_many datament for each partition in the DAL.
        partitions = defaultdict(list)

        for resource in resources:
            if resource is None:
                continue
            if common_values:
                resource.merge(common_values)
            partitions[tuple(resource.dirty)].append(resource)

        for resource_partition in partitions.values():
            records, _ids = [], []

            for resource in resource_partition:
                record = resource.dirty.copy()
                record.pop(REV_FIELD_NAME, None)
                record.pop(ID_FIELD_NAME, None)
                records.append(record)
                _ids.append(resource._id)

            store = cls.get_store()
            updated_records = store.dispatch('update_many', (_ids, records))

            for resource, record in zip(resource_partition, updated_records):
                resource.internal.state.update(record)
                resource.clean()

        return cls.Batch(resources)

    @classmethod
    def save_many(
        cls,
        resources: List['Resource'],
        depth: int = 0
    ) -> 'Batch':
        """
        Essentially a bulk upsert.
        """
        def seems_created(resource):
            return (
                (ID_FIELD_NAME in resource.internal.state) and
                (ID_FIELD_NAME not in resource.internal.state.dirty)
            )

        # partition resources into those that are "uncreated" and those which
        # simply need to be updated.
        to_update = []
        to_create = []
        for resource in resources:
            # TODO: merge duplicates
            if not seems_created(resource):
                to_create.append(resource)
            else:
                to_update.append(resource)

        # perform bulk create and update
        if to_create:
            created = cls.create_many(to_create)
        if to_update:
            updated = cls.update_many(to_update)

        retval = cls.Batch(to_update + to_create)

        if depth < 1:
            # base case. do not recurse on Resolvers
            return retval

        # aggregate and save all Resources referenced by all objects in
        # `resource` via their resolvers.
        class_2_objects = defaultdict(set)
        resolvers = cls.pybiz.resolvers.by_tag('fields', invert=True)
        for resolver in resolvers.values():
            for resource in resources:
                if resolver.name in resource.internal.state:
                    value = resource.internal.state[resolver.name]
                    entity_to_save = resolver.on_save(resolver, resource, value)
                    if entity_to_save:
                        if is_resource(entity_to_save):
                            class_2_objects[resolver.owner].add(
                                entity_to_save
                            )
                        else:
                            assert is_sequence(entity_to_save)
                            class_2_objects[resolver.owner].update(
                                entity_to_save
                            )

        # recursively call save_many for each type of Resource
        for biz_class, resources in class_2_objects.items():
            biz_class.save_many(resources, depth=depth-1)

        return retval


if __name__ == '__main__':
    from pybiz.schema import Schema, Field, String

    resolver = ResolverDecorator

    class Account(Resource):
        pass

    class User(Resource):
        email = String()

        @resolver(Account)
        def account(self, request):
            return request.query(first=True)

    request = User.account.select()
    query = User.select(User.email, request)
    assert request in query.selected.requests

    results = query()
    assert len(results) == 1

    user = results[0]
    account = getattr(user, 'account', None)
    assert isinstance(account, Account)
