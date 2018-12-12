from typing import Text, Tuple, List

from pybiz.predicate import (
    Predicate,
    ConditionalPredicate,
    BooleanPredicate,
)


class ComparableProperty(property):
    def __init__(self, key: Text, **kwargs):
        super().__init__(**kwargs)
        self._key = key

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
