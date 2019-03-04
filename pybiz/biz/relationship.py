from typing import Text, Type, Tuple, Dict

from appyratus.memoize import memoized_property
from appyratus.schema.fields import Field

from pybiz.util import is_bizobj
from pybiz.predicate import (
    Predicate,
    ConditionalPredicate,
    BooleanPredicate,
    OP_CODE,
)

from .internal.query import QuerySpecification
# TODO: rename "host" to source_type

class Relationship(object):
    """
    Instances of `Relationship` are declared as `BizObject` class attributes and
    are used to endow said classes with the ability to load and dump
    other `BizObject` objects and lists of objects recursively. For example,

    ```python3
    class Account(BizObject):

        # list of users at the account
        members = Relationship(
            lambda account: (User.account_id == account._id)
        )
    ```
    """

    def __init__(
        self,
        conditions,
        on_set=None,
        on_get=None,
        on_del=None,
        on_add=None,
        many=False,
        ordering=None,
        fields=None,
        offset: int = None,
        limit: int = None,
        private=False,
        lazy=True,
        readonly=False,
    ):
        def normalize_to_tuple(obj):
            if obj is not None:
                if isinstance(obj, (list, tuple)):
                    return tuple(obj)
                return (obj, )
            return tuple()

        self.many = many
        self.private = private
        self.lazy = lazy
        self.readonly = readonly

        self.conditions = normalize_to_tuple(conditions)
        self.on_set = normalize_to_tuple(on_set)
        self.on_get = normalize_to_tuple(on_get)
        self.on_del = normalize_to_tuple(on_del)
        self.on_add = normalize_to_tuple(on_add)

        # set in self.bind. Host is the BizObject class that hosts tis
        # relationship, and `name` is the relationship attribute on said class.
        self._biz_type = None
        self._name = None

        self._query_spec_sequence = []
        self._target_type_sequence = []
        self._is_bootstrapped = False
        self._registry = None

        self._order_by = normalize_to_tuple(ordering)
        self._limit = max(1, limit) if limit is not None else None
        self._offset = max(0, offset) if offset is not None else None
        self._fields = fields

    def __repr__(self):
        return '<{}({})>'.format(
            self.__class__.__name__,
            ', '.join([
                (self.biz_type.__name__ + '.' if self.biz_type else '')
                    + str(self._name) or '',
                'many={}'.format(self.many),
                'private={}'.format(self.private),
                'lazy={}'.format(self.lazy),
            ])
        )

    @property
    def name(self) -> Text:
        return self._name

    @property
    def join(self) -> Tuple:
        return self.conditions

    @property
    def biz_type(self) -> Type['BizObject']:
        return self._biz_type

    @property
    def target(self) -> Type['BizObject']:
        return self._target_type_sequence[-1]

    @property
    def spec(self) -> 'QuerySpecification':
        return self._query_spec_sequence[-1]

    @property
    def is_bootstrapped(self):
        return self._is_bootstrapped

    @property
    def registry(self):
        return self._registry

    def on_bootstrap(self):
        pass

    def bootstrap(
        self,
        biz_type: Type['BizObject'],
        registry: 'Registry' = None,
    ):
        self._registry = registry

        # this injects all BizObject class names into the condition functions'
        # lexical scopes. This mechanism helps avoid cyclic import dependencies
        # for the sake of defining relationships in BizObjects.
        if registry is not None:
            for func in self.conditions:
                func.__globals__.update(registry.manifest.types.biz)

        # resolve the BizObject classes to query in each condition and
        # prepare their QuerySpecifications.
        for idx, func in enumerate(self.conditions):
            mock = MockBizObject()
            predicate = func(mock)
            target_type = resolve_target(predicate)
            self._target_type_sequence.append(target_type)
            if idx > 0:
                spec = QuerySpecification(fields=mock.keys())
                self._query_spec_sequence.append(spec)

        # build the spec used for the final target_type.query call
        final_spec = QuerySpecification.prepare(self._fields, self.target)
        final_spec.limit = self._limit
        final_spec.offset = self._offset
        final_spec.order_by = self._order_by
        for item in final_spec.order_by:
            final_spec.fields.add(item().key)

        self._query_spec_sequence.append(final_spec)

        self.on_bootstrap()

        self._is_bootstrapped = True

    def query(self, caller: 'BizObject'):
        """
        Execute a chain of queries to fetch the target Relationship data.
        """
        # execute the sequence of queries
        target = caller

        for idx, func in enumerate(self.conditions):
            predicate = func(target)
            target_type = self._target_type_sequence[idx]
            spec = self._query_spec_sequence[idx]
            target = target_type.query(predicate=predicate, specification=spec)
            if not target:
                if self.many:
                    return self.biz_type.BizList([], self, owner)
                else:
                    return None

        target.relationship = self
        target.bizobj = caller

        # return one or more depending on self.many
        if self.many:
            return target if target else self.biz_type.BizList([], self, owner)
        else:
            return target[0] if target else None

    def associate(self, biz_type: Type['BizObject'], name: Text):
        """
        This is called by the BizObject metaclass when associating its set of
        relationship objects with the owner BizObject class.
        """
        self._biz_type = biz_type
        self._name = name


class MockBizObject(object):
    """
    Used internally by `Relationship` in order to be able to execute and inspect
    `Predicate` objects before they are executed by real queries. See:
    `Relationship.query`.
    """

    def __init__(self):
        # 'attrs' is the set of attr names that the program attempted to access
        # on this instance:
        self.attrs = set()

        # `inner` is used as a dummy iterator element to be used when the
        # program tries to access contained elements, as if this instance were a
        # list.
        self.inner = None

    def __getattr__(self, key):
        self.attrs.add(key)
        return key

    def __getitem__(self, key):
        return getattr(self, key)

    def __iter__(self):
        if self.inner is None:
            self.inner = MockBizObject()
        return iter([self.inner])

    def __contains__(self, value):
        return True

    def keys(self):
        keys = set(self.attrs)
        if self.inner:
            keys |= self.inner.keys()
        return keys


def resolve_target(predicate):
    if len(predicate.targets) > 1:
        # each root-level join predicate should contain exactly one
        # BizObject class in its `targets` set because this is the class
        # we assume is the type of BizObject being queried in this
        # iteration.
        raise ValueError(
            'ambiguous target BizObject in self.conditions'
        )
    if not predicate.targets:
        raise ValueError(
            'no target BizObject could be resolved'
        )
    return predicate.targets[0]
