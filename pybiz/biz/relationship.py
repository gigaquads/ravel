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
        private=False,
        lazy=True,
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

        self.conditions = normalize_to_tuple(conditions)
        self.on_set = normalize_to_tuple(on_set)
        self.on_get = normalize_to_tuple(on_get)
        self.on_del = normalize_to_tuple(on_del)
        self.on_add = normalize_to_tuple(on_add)
        self.ordering = normalize_to_tuple(ordering)

        # set in self.bind. Host is the BizObject class that hosts tis
        # relationship, and `name` is the relationship attribute on said class.
        self._biz_type = None
        self._name = None

        # `_query_fields` is a sequence of field name sets to be used as the
        # `fields` argument to each query predicate after the first, assuming
        # this is multi-query relationship.
        self._query_fields = []

    def __repr__(self):
        return '<{}({})>'.format(
            self.__class__.__name__,
            ', '.join([
                (self.biz_type.__name__ + '.' if self.biz_type else '')
                    + self._name or '',
                'many={}'.format(self.many),
                'private={}'.format(self.private),
                'lazy={}'.format(self.lazy),
            ])
        )

    def query(
        self,
        owner: 'BizObject',
        specification: QuerySpecification = None,
    ):
        """
        Execute a chain of queries to fetch the target Relationship data.
        """
        # TODO: do this just once on registry bootstrap instead
        # dynamically add biz object classes to namespace of predicate funcs
        # so that we don't have to structure our modules oddly to avoid cyclic
        # imports for BizObjects.
        if owner.registry is not None:
            for predicate_func in self.conditions:
                predicate_func.__globals__.update(owner.registry.types.biz)

        predicate = self.conditions[-1](MockBizObject())
        target = self._resolve_target(predicate)

        specification = QuerySpecification.prepare(
            specification, self.target
        )

        # build up the sequence of field name sets to query
        if not self._query_fields:
            for idx, func in enumerate(self.conditions[1:]):
                mock = MockBizObject()
                func(mock)
                self._query_fields.append(set(mock.keys()))

        # execute the sequence of queries
        obj = owner
        for idx, func in enumerate(self.conditions):
            if not obj:
                if self.many:
                    return self.biz_type.BizList([], self, owner)
                else:
                    return None

            if (not idx) or (not self.many):
                obj_type = obj.__class__
            else:
                obj_type = obj.biz_type

            # `target` is the BizObject class we are querying
            predicate = func(obj)
            target_type = self._resolve_target(predicate)

            # get the field name set to use in this query
            if func is not self.conditions[-1]:
                field_names = self._query_fields[idx]
                spec = QuerySpecification(fields=field_names)
            else:
                spec = QuerySpecification.prepare(
                    specification, obj_type,
                )

            # ensure that any key we are ordering by
            # is included in fields
            if spec.order_by:
                for item in spec.order_by:
                    spec.fields.add(item.key)

            # obj is always a BizList
            obj = target_type.query(
                predicate=predicate, specification=spec
            )

        if obj is not owner:
            obj.relationship = self
            obj.bizobj = owner

        # set relationship's default order by on spec
        if obj and self.ordering and (not spec.order_by):
            if callable(self.ordering):
                spec.order_by = self.ordering(obj)
            else:
                spec.order_by = self.ordering

        # return one or more depending on self.many
        if self.many:
            return obj if obj else self.biz_type.BizList([], self, owner)
        else:
            return obj[0] if obj else None

    def bind(self, biz_type: Type['BizObject'], name: Text):
        self.biz_type = biz_type
        self._name = name

    @property
    def name(self) -> Text:
        return self._name

    @property
    def join(self) -> Tuple:
        return self.conditions

    @memoized_property
    def target(self) -> Type['BizObject']:
        # TODO: rename to target_type
        predicate = self.conditions[-1](MockBizObject())
        return self._resolve_target(predicate)

    @staticmethod
    def _resolve_target(predicate):
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
