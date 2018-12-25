from typing import Text, Tuple, List

from pybiz.predicate import (
    Predicate,
    ConditionalPredicate,
    BooleanPredicate,
)


class FieldProperty(property):
    def __init__(self, field: 'Field', **kwargs):
        super().__init__(**kwargs)
        self._field = field
        self._key = field.source

    def __eq__(self, other: Predicate) -> Predicate:
        return ConditionalPredicate(self._key, '=', other)

    def __ne__(self, other: Predicate) -> Predicate:
        return ConditionalPredicate(self._key, '!=', other)

    def __lt__(self, other: Predicate) -> Predicate:
        return ConditionalPredicate(self._key, '<', other)

    def __le__(self, other: Predicate) -> Predicate:
        return ConditionalPredicate(self._key, '<=', other)

    def __gt__(self, other: Predicate) -> Predicate:
        return ConditionalPredicate(self._key, '>', other)

    def __ge__(self, other: Predicate) -> Predicate:
        return ConditionalPredicate(self._key, '>=', other)

    def __ge__(self, other: Predicate) -> Predicate:
        return ConditionalPredicate(self._key, '>=', other)

    def is_in(self, others: List[Predicate]) -> Predicate:
        return ConditionalPredicate(self._key, 'in', others)

    def is_not_in(self, others: List[Predicate]) -> Predicate:
        return ConditionalPredicate(self._key, 'nin', others)

    @property
    def key(self) -> Text:
        return self._key

    @property
    def asc(self) -> Tuple:
        return (self._key, +1)

    @property
    def desc(self) -> Tuple:
        return (self._key, -1)

    @classmethod
    def build(
        cls,
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

        return cls(field, fget=fget, fset=fset, fdel=fdel)
