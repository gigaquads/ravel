import uuid

import pybiz.biz

from typing import Text, Tuple, List, Type, Callable

from pybiz.util.misc_functions import (
    is_biz_obj,
    flatten_sequence,
    get_class_name,
)
from pybiz.util.loggers import console
from pybiz.constants import ID_FIELD_NAME
from pybiz.predicate import (
    Predicate,
    ConditionalPredicate,
    BooleanPredicate,
    OP_CODE,
)


class FieldProperty(property):
    def __init__(self, biz_class: Type['BizObject'], field: 'Field'):
        super().__init__(fget=self.fget, fset=self.fset, fdel=self.fdel)
        self._field_name = field.name
        self._biz_class = biz_class
        self._hash = uuid.uuid4().int

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

    def including(self, *others) -> Predicate:
        others = flatten_sequence(others)
        others = {obj._id if is_biz_obj(obj) else obj for obj in others}
        return self._build_predicate(OP_CODE.INCLUDING, others)

    def excluding(self, *others) -> Predicate:
        others = flatten_sequence(others)
        others = {obj._id if is_biz_obj(obj) else obj for obj in others}
        return self._build_predicate(OP_CODE.EXCLUDING, others)

    @property
    def biz_class(self):
        return self._biz_class

    @property
    def field(self):
        return self._biz_class.Schema.fields[self._field_name]

    @property
    def asc(self) -> 'OrderBy':
        return pybiz.biz.OrderBy(self.field.source, desc=False)

    @property
    def desc(self) -> 'OrderBy':
        return pybiz.biz.OrderBy(self.field.source, desc=True)

    def transform(
        self,
        *callbacks: Tuple[Callable],
        **params
    ) -> 'FieldPropertyQuery':
        from pybiz.biz.query import FieldPropertyQuery
        return FieldPropertyQuery(
            fprop=self,
            alias=self.field.name,
            params=params,
            callbacks=callbacks,
        )

    def fget(self, biz_obj):
        # try to lazy load the field value
        is_loaded = self.field.name in biz_obj.internal.state
        exists_in_dao = ID_FIELD_NAME in biz_obj.internal.state

        if (not is_loaded) and exists_in_dao:
            if self.field.meta.get('lazy', True):
                field_names_to_load = (
                    biz_obj.schema.fields.keys() - biz_obj.internal.state.keys()
                )
                field_source_names_to_load = {
                    biz_obj.schema.fields[k].source
                    for k in field_names_to_load
                }
                console.debug(
                    message=(
                        f'lazy loading fields via '
                        f'{get_class_name(self.biz_class)}.{self.field.name}'
                    ),
                    data={
                        'instance': biz_obj._id,
                        'class': get_class_name(self.biz_class),
                        'fields': field_source_names_to_load,
                    }
                )
                biz_obj.load(field_source_names_to_load)

        return biz_obj.internal.state.get(self.field.name)

    def fset(self, biz_obj, value):
        key = self.field.name
        if value is not None:
            value, error = self.field.process(value)
            if error:
                raise ValueError(
                    f'error setting {key} to {value}: {error}'
                )
            biz_obj.internal.state[key] = value
        elif self.field.nullable:
            biz_obj.internal.state[key] = None
        else:
            raise AttributeError(key)

    def fdel(self, biz_obj):
        key = self.field.name
        if self.field.required:
            raise AttributeError(
                f'cannot delete required field value: {key}'
            )
        else:
            biz_obj.internal.state.pop(key, None)
