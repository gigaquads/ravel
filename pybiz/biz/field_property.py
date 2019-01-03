from typing import Text, Tuple, List, Type

from pybiz.util import is_bizobj
from pybiz.predicate import (
    Predicate,
    ConditionalPredicate,
    BooleanPredicate,
)


class FieldProperty(property):
    def __init__(self,
        target: Type['BizObject'],
        field: 'Field',
        **kwargs
    ):
        super().__init__(**kwargs)
        self._field = field
        self._target = target

    def _build_predicate(self, op, other):
        return ConditionalPredicate(op, self, other)

    def __repr__(self):
        target_name = None
        if self.target:
            target_name = self.target.__name__
        field_name = None
        if self.field:
            field_name = self.field.name
        return '<FieldProperty({}{})>'.format(
            target_name + '.' if target_name else '',
            field_name or ''
        )

    def __eq__(self, other: Predicate) -> Predicate:
        return self._build_predicate('=', other)

    def __ne__(self, other: Predicate) -> Predicate:
        return self._build_predicate('!=', other)

    def __lt__(self, other: Predicate) -> Predicate:
        return self._build_predicate('<', other)

    def __le__(self, other: Predicate) -> Predicate:
        return self._build_predicate('<=', other)

    def __gt__(self, other: Predicate) -> Predicate:
        return self._build_predicate('>', other)

    def __ge__(self, other: Predicate) -> Predicate:
        return self._build_predicate('>=', other)

    def includes(self, others: List[Predicate]) -> Predicate:
        others = {obj._id if is_bizobj(obj) else obj for obj in others}
        return self._build_predicate('in', others)

    def excludes(self, others: List[Predicate]) -> Predicate:
        others = {obj._id if is_bizobj(obj) else obj for obj in others}
        return self._build_predicate('ex', others)

    @property
    def target(self):
        return self._target

    @property
    def field(self):
        return self._field

    @property
    def key(self) -> Text:
        return self._field.source

    @property
    def asc(self) -> Tuple:
        return (self._key, +1)

    @property
    def desc(self) -> Tuple:
        return (self._key, -1)

    @classmethod
    def build(
        cls,
        target,
        field: 'Field',
    ) -> 'FieldProperty':
        """
        """
        key = field.name

        def fget(self):
            if (key not in self.data) and '_id' in self.data:
                # try to lazy load the field value
                field = self.schema.fields.get(key)
                if field and field.meta.get('lazy', True):
                    record = self.dao.fetch(
                        _id=self.data['_id'],
                        fields={field.source}
                    )
                    if record:
                        self.data[key] = record[key]
            return self[key]

        def fset(self, value):
            self[key] = value

        def fdel(self):
            del self[key]

        return cls(target, field, fget=fget, fset=fset, fdel=fdel)
