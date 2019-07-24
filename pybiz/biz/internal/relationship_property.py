from typing import Text, Type, Tuple

from pybiz.util import is_sequence
from pybiz.util.loggers import console
from pybiz.exc import RelationshipError
from pybiz.predicate import (
    ConditionalPredicate,
    BooleanPredicate,
    OP_CODE,
)

from ..query import Query
from ..relationship import Relationship
from ..biz_list import BizList


class RelationshipProperty(property):

    @classmethod
    def build(cls, relationship: 'Relationship') -> 'RelationshipProperty':
        """
        Build and return a `RelationshipProperty`, that validates the data on
        getting/setting and lazy-loads data on get.
        """
        return cls(relationship)

    def __init__(self, relationship, **kwargs):
        super().__init__(fget=self.on_get, fset=self.on_set, fdel=self.on_del, **kwargs)
        self.relationship = relationship

    def __repr__(self):
        if self.relationship is not None:
            return repr(self.relationship).replace(
                'Relationship', 'RelationshipProperty'
            )
        else:
            return '<RelationshipProperty>'

    def select(self, *targets) -> 'Query':
        rel = self.relationship
        query = Query(rel.target_biz_type, rel.name)
        return (
            query
                .select(targets)
                .order_by(rel.order_by)
                .limit(rel.limit)
                .offset(rel.offset)
            )

    def on_get(self, source):
        """
        Return the related BizObject instance or list.
        """
        rel = self.relationship
        if rel.name not in source.related:
            if rel.lazy:
                # fetch all fields
                console.debug(
                    message='lazy loading relationship',
                    data={
                        'relationship': str(rel)
                    }
                )
                value = rel.query(source)
                source.related[rel.name] = value

        default = rel.target_biz_type.BizList([], rel, source) if rel.many else None
        value = source.related.get(rel.name, default)

        for cb_func in rel.on_get:
            cb_func(source, value)

        return value

    def on_set(self, source, target):
        """
        Set the related BizObject or list, enuring that a list can't be
        assigned to a Relationship with many == False and vice versa.
        """
        rel = self.relationship

        if rel.readonly:
            raise RelationshipError(f'{rel} is read-only')

        if target is None and rel.many:
            target = rel.target_biz_type.BizList([], rel, target)
        elif is_sequence(target):
            target = rel.target.BizList(target, rel, target)

        is_scalar = not isinstance(target, BizList)
        expect_scalar = not rel.many

        if is_scalar and not expect_scalar:
            raise ValueError(
                'relationship "{}" must be a sequence because '
                'relationship.many is True'.format(rel.name)
            )
        elif (not is_scalar) and expect_scalar:
            raise ValueError(
                'relationship "{}" cannot be a BizObject because '
                'relationship.many is False'.format(rel.name)
            )

        source.related[rel.name] = target
        for cb_func in rel.on_set:
            cb_func(source, target)

    def on_del(self, source):
        """
        Remove the related BizObject or list. The field will appear in
        dump() results. You must assign None if you want to None to appear.
        """
        rel = self.relationship
        if rel.name in source.related:
            target = source.related.pop(rel.name)
            for cb_func in rel.on_del:
                cb_func(source, target)
