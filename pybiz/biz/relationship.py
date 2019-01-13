import pybiz.biz

from typing import Text, Type, Tuple

from appyratus.memoize import memoized_property
from appyratus.schema.fields import Field

from pybiz.predicate import (
    Predicate,
    ConditionalPredicate,
    BooleanPredicate,
    OP_CODE,
)

from .query import QuerySpecification

# TODO: rename "host" to something more clear

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
        joins,
        many=False,
        private=False,
        lazy=True,
        on_set=None,
        on_get=None,
        on_del=None,
    ):
        self.many = many
        self.private = private
        self.lazy = lazy
        self.on_set = on_set
        self.on_get = on_get
        self.on_del = on_del

        # ensure `joins` is a tuple of callables that return predicates
        if callable(joins):
            self._joins = (joins, )
        elif isinstance(joins, (list, tuple)):
            self._joins = tuple(joins)

        assert isinstance(self._joins, tuple)

        # set in self.bind. Host is the BizObject class that hosts tis
        # relationship, and `name` is the relationship attribute on said class.
        self._host = None
        self._name = None

        # `_query_fields` is a sequence of field name sets to be used as the
        # `fields` argument to each join predicate after the first, assuming
        # this is multi-join relationship.
        self._query_fields = []

    def __repr__(self):
        return '<{}({})>'.format(
            self.__class__.__name__,
            ', '.join([
                (self._host.__name__ + '.' if self._host else '')
                    + self._name or '',
                'many={}'.format(self.many),
                'private={}'.format(self.private),
                'lazy={}'.format(self.lazy),
            ])
        )

    def query(self, source, specification):
        """
        Execute a chain of queries to fetch the target Relationship data.
        """
        # build up the sequence of field name sets to query
        if not self._query_fields:
            for idx, func in enumerate(self._joins[1:]):
                mock = MockBizObject()
                func(mock)
                self._query_fields.append(set(mock.keys()))

        # execute the sequence of "join" queries...
        for idx, func in enumerate(self._joins):
            if not source:
                return [] if self.many else None

            # `target` is the BizObject class we are querying
            predicate = func(source)
            target = self._resolve_target(predicate)

            # get the field name set to use in this query
            if func is not self._joins[-1]:
                field_names = self._query_fields[idx]
                spec = QuerySpecification(fields=field_names)
            else:
                spec = specification

            # do it!
            source = target.query(predicate=predicate, specification=spec)

        # return one or more depending on self.many
        if self.many:
            return source if source else []
        else:
            return source[0] if source else None

    def bind(self, host: Type['BizObject'], name: Text):
        self._host = host
        self._name = name

    @property
    def name(self) -> Text:
        return self._name

    @property
    def joins(self) -> Tuple:
        return self._joins

    @property
    def host(self) -> Type['BizObject']:
        return self._host

    @memoized_property
    def target(self) -> Type['BizObject']:
        predicate = self._joins[-1](MockBizObject())
        return self._resolve_target(predicate)

    @staticmethod
    def _resolve_target(predicate):
        if len(predicate.targets) > 1:
            # each root-level join predicate should contain exactly one
            # BizObject class in its `targets` set because this is the class
            # we assume is the type of BizObject being queried in this
            # iteration.
            raise ValueError(
                'ambiguous target BizObject in self.joins'
            )
        if not predicate.targets:
            raise ValueError(
                'no target BizObject could be resolved'
            )
        return predicate.targets[0]


class RelationshipProperty(property):
    def __init__(self, relationship, **kwargs):
        super().__init__(**kwargs)
        self.relationship = relationship

    def __repr__(self):
        if self.relationship is not None:
            return repr(self.relationship).replace(
                'Relationship', 'RelationshipProperty'
            )
        else:
            return '<RelationshipProperty>'


    @classmethod
    def build(
        cls,
        relationship: 'Relationship'
    ) -> 'RelationshipProperty':
        """
        Build and return a `RelationshipProperty`, that validates the data on
        getting/setting and lazy-loads data on get.
        """
        rel = relationship
        key = relationship.name

        def is_scalar_value(obj):
            # just a helper func
            return not isinstance(obj, (list, set, tuple))

        def fget(self):
            """
            Return the related BizObject instance or list.
            """
            if key not in self._related:
                if rel.lazy and rel.query:
                    # lazily fetch the related data, eagerly selecting all fields
                    related_obj = rel.query(self, {'*'})
                    setattr(self, key, related_obj)

            default = [] if rel.many else None
            value = self._related.get(key, default)

            if rel.on_get is not None:
                rel.on_get(self, value)

            return value

        def fset(self, value):
            """
            Set the related BizObject or list, enuring that a list can't be
            assigned to a Relationship with many == False and vice versa.
            """
            rel = self.relationships[key]
            is_scalar = is_scalar_value(value)
            expect_scalar = not rel.many

            if (not expect_scalar) and isinstance(value, dict):
                # assume that the value is a map from id to bizobj, so
                # convert the dict value set into a list to use as the
                # value set for the Relationship.
                value = list(value.values())

            if is_scalar and not expect_scalar:
                    raise ValueError(
                        'relationship "{}" must be a sequence because '
                        'relationship.many is True'.format(key)
                    )
            elif (not is_scalar) and expect_scalar:
                raise ValueError(
                    'relationship "{}" cannot be a BizObject because '
                    'relationship.many is False'.format(key)
                )

            self._related[key] = value

            if (not rel.many) and rel.joins:
                RelationshipProperty.set_foreign_keys(self, value, rel)

            if rel.on_set is not None:
                rel.on_set(self, value)

        def fdel(self):
            """
            Remove the related BizObject or list. The field will appeear in
            dump() results. You must assign None if you want to None to appear.
            """
            value = self._related[key]

            del self._related[key]

            if rel.on_del is not None:
                rel.on_del(self, value)

        return cls(relationship, fget=fget, fset=fset, fdel=fdel)

    @staticmethod
    def set_foreign_keys(bizobj, related_bizobj, rel):
        """
        When setting a relationship, we might be able to set any fields declared
        on the host bizobj based on the contents of the Relationship's join
        predicates. For example, a node might have a parent_id field, which we
        would want to set when doing somehing like child.parent = parent (we
        would want child.parent_id = parent._id to be performed automatically).
        """
        pred = rel.joins[0](MockBizObject())
        if isinstance(pred, ConditionalPredicate):
            if pred.op == OP_CODE.EQ:
                attr_name = pred.value
                related_attr_name = pred.field.name
                related_value = getattr(related_bizobj, related_attr_name, None)
                setattr(bizobj, attr_name, related_value)


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
