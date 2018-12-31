import pybiz.biz

from typing import Text, Type, Tuple

from appyratus.memoize import memoized_property
from appyratus.schema.fields import Field

from pybiz.predicate import (
    Predicate,
    ConditionalPredicate,
    BooleanPredicate,
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
    ):
        self.many = many
        self.private = private
        self.lazy = lazy

        # ensure `joins` is a tuple of callables that return predicates
        if isinstance(joins, Predicate):
            self._joins = (joins, )
        else:
            self._joins = tuple(joins)

        # set in self.bind. Host is the BizObject class that hosts tis
        # relationship, and `name` is the relationship attribute on said class.
        self._host = None
        self._name = None

        # `_query_fields` is a sequence of field name sets to be used as the
        # `fields` argument to each join predicate after the first, assuming
        # this is multi-join relationship.
        self._query_fields = []

    def __repr__(self):
        return '<{}({}{})>'.format(
            self.__class__.__name__,
            self._host.__name__ + '.' if self._host else '',
            self._name or '',
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
                    # make sure we are setting an instance object or collection
                    # of objects according to the field's "many" flag.
                    is_scalar = is_scalar_value(related_obj)
                    expect_scalar = not rel.many
                    if is_scalar and not expect_scalar:
                        raise ValueError(
                            'relationship "{}" query returned an object but '
                            'expected a sequence because relationship.many '
                            'is True'.format(key)
                        )
                    elif (not is_scalar) and expect_scalar:
                        raise ValueError(
                            'relationship "{}" query returned a sequence but '
                            'expected a BizObject because relationship.many '
                            'is False'.format(key)
                        )
                    self._related[key] = related_obj

            default = [] if rel.many else None
            return self._related.get(key, default)

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

        def fdel(self):
            """
            Remove the related BizObject or list. The field will appeear in
            dump() results. You must assign None if you want to None to appear.
            """
            del self._related[k]

        return cls(relationship, fget=fget, fset=fset, fdel=fdel)


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
        return None

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
