from typing import Text, Type, Tuple

from pybiz.predicate import (
    ConditionalPredicate,
    BooleanPredicate,
    OP_CODE,
)

from .relationship import Relationship, MockBizObject


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

            if (not rel.many) and rel.join:
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
        pred = rel.join[0](MockBizObject())
        if isinstance(pred, ConditionalPredicate):
            if pred.op == OP_CODE.EQ:
                attr_name = pred.value
                related_attr_name = pred.field.name
                related_value = getattr(related_bizobj, related_attr_name, None)
                setattr(bizobj, attr_name, related_value)
