import pybiz.biz

from typing import Text, Tuple, List, Type

from appyratus.schema.fields import Uuid

from pybiz.util.misc_functions import is_bizobj
from pybiz.util.loggers import console
from pybiz.predicate import (
    Predicate,
    ConditionalPredicate,
    BooleanPredicate,
    OP_CODE,
)


class FieldProperty(property):
    def __init__(self, biz_class: Type['BizObject'], field: 'Field'):
        super().__init__(fget=self.fget, fset=self.fset, fdel=self.fdel)
        self._field = field
        self._biz_class = biz_class
        self._hash = Uuid.next_id().int

    def __repr__(self):
        type_name = None
        field_name = None

        if self.biz_class:
            type_name = self.biz_class.__name__
        if self.field:
            field_name = self.field.name

        return '<FieldProperty({}{})>'.format(
            type_name + '.' if type_name else '',
            field_name or ''
        )

    def __hash__(self):
        return self._hash

    def _build_predicate(self, op, other):
        return ConditionalPredicate(op, self, other)

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
    def biz_class(self):
        return self._biz_class

    @property
    def field(self):
        return self._field

    @property
    def asc(self) -> 'OrderBy':
        return pybiz.biz.OrderBy(self.field.source, desc=False)

    @property
    def desc(self) -> 'OrderBy':
        return pybiz.biz.OrderBy(self.field.source, desc=True)

    def fget(self, bizobj):
        # try to lazy load the field value
        is_loaded = self.field.name in bizobj.internal.state
        exists_in_dao = '_id' in bizobj.internal.state

        if (not is_loaded) and exists_in_dao:
            if self.field.meta.get('lazy', True):
                field_names_to_load = (
                    bizobj.schema.fields.keys() - bizobj.internal.state.keys()
                )
                field_source_names_to_load = {
                    bizobj.schema.fields[k].source
                    for k in field_names_to_load
                }
                console.debug(
                    message=f'lazy loading fields',
                    data={
                        'object': str(bizobj),
                        'fields': field_source_names_to_load,
                    }
                )
                bizobj.load(field_source_names_to_load)

        return bizobj.internal.state.get(self.field.name)

    def fset(self, bizobj, value):
        key = self.field.name
        if value is not None:
            value, error = self.field.process(value)
            if error:
                raise ValueError(f'error setting {key} to {value}: {error}')
            bizobj.internal.state[key] = value
        elif self.field.nullable:
            bizobj.internal.state[key] = None
        else:
            raise AttributeError(key)

    def fdel(self, bizobj):
        key = self.field.name
        if self.field.required:
            raise AttributeError(f'cannot delete required field value: {key}')
        else:
            bizobj.internal.state.pop(key, None)
