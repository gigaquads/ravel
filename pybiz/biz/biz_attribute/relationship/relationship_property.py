from typing import Set

from pybiz.exceptions import RelationshipError
from pybiz.util.misc_functions import is_sequence, is_bizlist
from pybiz.util.loggers import console

from ..biz_attribute import BizAttributeProperty
from ...query import Query


class RelationshipProperty(BizAttributeProperty):

    @property
    def relationship(self):
        return self.biz_attr

    def select(self, *selectors) -> Query:
        """
        Return a Query object targeting this property's Relationship BizObject
        type, initializing the Query parameters to those set in the
        Relationship's constructor. The "where" conditions are computed during
        Relationship.execute().
        """
        rel = self.relationship
        return Query(
            biz_class=rel.target_biz_class,
            alias=rel.name,
            select=selectors or set(rel.target_biz_class.Schema.fields.keys()),
            order_by=rel.order_by,
            limit=rel.limit,
            offset=rel.offset,
        )

    def fget(self, source: 'BizObject') -> 'BizThing':
        """
        Return the memoized BizObject instance or list.
        """
        rel = self.relationship
        selectors = set(rel.target_biz_class.Schema.fields.keys())
        default = (
            rel.target_biz_class.BizList([], rel, source) if rel.many else None
        )

        # get or lazy load the BizThing
        biz_thing = super().fget(source, select=selectors)
        if not biz_thing:
            biz_thing = default

        # if the data was lazy loaded for the first time and returns a BizList,
        # we want to set the source and relationship properties on it.
        if rel.many:
            if biz_thing.source is None:
                biz_thing.source = source
            if biz_thing.relationship is None:
                biz_thing.relationship = rel

        # perform callbacks set on Relationship ctor
        for cb_func in rel.on_get:
            cb_func(source, biz_thing)

        return biz_thing

    def fset(self, source: 'BizObject', target: 'BizThing'):
        """
        Set the memoized BizObject or list, enuring that a list can't be
        assigned to a Relationship with many == False and vice versa.
        """
        rel = self.relationship

        if source.internal.state.get(rel.name) and rel.readonly:
            raise RelationshipError(f'{rel} is read-only')

        if target is None and rel.many:
            target = rel.target_biz_class.BizList([], rel, self)
        elif is_sequence(target):
            target = rel.target_biz_class.BizList(target, rel, self)

        is_scalar = not is_bizlist(target)
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

        super().fset(source, target)

        for cb_func in rel.on_set:
            cb_func(source, target)

    def fdel(self, source: 'BizObject'):
        """
        Remove the memoized BizObject or list. The field will appear in
        dump() results. You must assign None if you want to None to appear.
        """
        super().fdel(source)
        for cb_func in self.relationship.on_del:
            cb_func(source, target)
