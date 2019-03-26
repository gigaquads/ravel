from typing import Text, Tuple, List, Type

from pybiz.util import is_bizobj
from pybiz.util.loggers import console
from pybiz.predicate import (
    Predicate,
    ConditionalPredicate,
    BooleanPredicate,
    OP_CODE,
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
        return self._build_predicate(OP_CODE.EQ, other)

    def __ne__(self, other: Predicate) -> Predicate:
        return self._build_predicate(OP_CODE.NEQ, other)

    def __lt__(self, other: Predicate) -> Predicate:
        return self._build_predicate(OP_CODE.LT, other)

    def __le__(self, other: Predicate) -> Predicate:
        return self._build_predicate(OP_CODE.LEQ, other)

    def __gt__(self, other: Predicate) -> Predicate:
        return self._build_predicate(OP_CODE.GT, other)

    def __ge__(self, other: Predicate) -> Predicate:
        return self._build_predicate(OP_CODE.GEQ, other)

    def including(self, others: List) -> Predicate:
        others = {obj._id if is_bizobj(obj) else obj for obj in others}
        return self._build_predicate(OP_CODE.INCLUDING, others)

    def excluding(self, others: List) -> Predicate:
        others = {obj._id if is_bizobj(obj) else obj for obj in others}
        return self._build_predicate(OP_CODE.EXCLUDING, others)

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
    def asc(self) -> 'OrderBy':
        return OrderBy(self.key, desc=False)

    @property
    def desc(self) -> 'OrderBy':
        return OrderBy(self.key, desc=True)

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
            # raise if the field is not declared in the schema
            if key not in self.schema.fields:
                raise AttributeError(key)

            # try to lazy load the field value
            if (key not in self._data) and ('_id' in self._data):
                field = self.schema.fields.get(key)
                if field and field.meta.get('lazy', True):
                    unloaded_fields = list(
                        self.schema.fields.keys() - self._data.keys()
                    )
                    console.debug(
                        message=f'lazy loading fields',
                        data={
                            'object': str(self),
                            'fields': unloaded_fields,
                        }
                    )
                    self.load(unloaded_fields)

            if key in self._data:
                return self._data[key]
            else:
                return None

        def fset(self, value):
            field = self.schema.fields.get(key)
            if field is not None:
                if not (value is None and field.nullable):
                    value, error = field.process(value)
                    if error:
                        raise ValueError(f'error setting {key}: {error}')
                self._data[key] = value
            else:
                raise AttributeError(key)

        def fdel(self):
            if self.schema.fields[key].required:
                raise AttributeError(
                    'cannot delete required field value: {}'.format(key)
                )
            else:
                self._data.pop(key, None)

        return cls(target, field, fget=fget, fset=fset, fdel=fdel)


class OrderBy(object):
    def __init__(self, key: Text, desc=False):
        self.key = key
        self.desc = desc

    @property
    def asc(self):
        return not self.desc
