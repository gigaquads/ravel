import pybiz.biz.biz_object as biz_object

from typing import Text, Type, Tuple

from pybiz.util import is_sequence
from pybiz.predicate import (
    ConditionalPredicate,
    BooleanPredicate,
    OP_CODE,
)
from pybiz.biz.relationship import MockBizObject

from .query import QuerySpecification
from ..relationship import Relationship
from ..biz_list import BizList


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
    def build(cls, relationship: 'Relationship') -> 'RelationshipProperty':
        """
        Build and return a `RelationshipProperty`, that validates the data on
        getting/setting and lazy-loads data on get.
        """
        rel = relationship
        key = relationship.name

        def fget(self):
            """
            Return the related BizObject instance or list.
            """
            if key not in self._related:
                if rel.lazy:
                    # fetch all fields
                    related_obj = rel.query(self)
                    setattr(self, key, related_obj)
                    for cb_func in rel.on_add:
                        if rel.many:
                            for bizobj in related_obj:
                                cb_func(self, bizobj)
                        else:
                            cb_func(self, related_obj)

            default = self.BizList([], rel, self) if rel.many else None
            value = self._related.get(key, default)

            for cb_func in rel.on_get:
                cb_func(self, value)

            return value

        def fset(self, value):
            """
            Set the related BizObject or list, enuring that a list can't be
            assigned to a Relationship with many == False and vice versa.
            """
            rel = self.relationships[key]

            if value is None and rel.many:
                value = rel.target.BizList([], rel, self)
            elif is_sequence(value):
                value = rel.target.BizList(value, rel, self)

            is_scalar = not isinstance(value, BizList)
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

            for cb_func in rel.on_set:
                cb_func(self, value)

        def fdel(self):
            """
            Remove the related BizObject or list. The field will appeear in
            dump() results. You must assign None if you want to None to appear.
            """
            value = self._related[key]
            del self._related[key]
            for cb_func in rel.on_del:
                cb_func(self, value)

        return cls(relationship, fget=fget, fset=fset, fdel=fdel)
