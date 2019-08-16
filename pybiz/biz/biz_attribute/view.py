from typing import Text, Dict, Callable, Type, Tuple

from pybiz.schema import Field
from pybiz.util.misc_functions import is_sequence, normalize_to_tuple
from pybiz.util.loggers import console

from .biz_attribute import BizAttribute, BizAttributeProperty


class View(BizAttribute):
    def __init__(
        self,
        load: Callable,
        transform: Callable = None,
        field: Field = None,
        on_set: Tuple[Callable] = None,
        on_get: Tuple[Callable] = None,
        on_del: Tuple[Callable] = None,
        *args,
        **kwargs
    ):
        super().__init__(*args, **kwargs)
        self._load = load
        self._transform = transform
        self._field = field
        self._on_get = normalize_to_tuple(on_get) if on_get else tuple()
        self._on_set = normalize_to_tuple(on_set) if on_set else tuple()
        self._on_del = normalize_to_tuple(on_del) if on_del else tuple()

    @property
    def order_key(self):
        return 10

    @property
    def category(self):
        return 'view'

    @property
    def field(self) -> Field:
        return self._field

    @property
    def on_get(self) -> Tuple[Callable]:
        return self._on_get

    @property
    def on_set(self) -> Tuple[Callable]:
        return self._on_set

    @property
    def on_del(self) -> Tuple[Callable]:
        return self._on_del

    def build_property(self) -> 'ViewProperty':
        return ViewProperty(self)

    def execute(self, caller: 'BizObject', *args, **kwargs) -> object:
        data = self._load(caller, *args, **kwargs)
        if self._transform is not None:
            data = self._transform(caller, data, *args, **kwargs)
            if self._field is not None:
                data, errors = self._field.process(data)
                if errors:
                    raise ValueError('validation error')  # TODO: raise validation error
        return data


class ViewProperty(BizAttributeProperty):

    @property
    def view(self) -> 'View':
        return self.biz_attr

    def fget(self, source: 'BizObject'):
        value = super().fget(source)
        for func in self.biz_attr.on_get:
            func(source, value)
        return value

    def fset(self, source: 'BizObject', value):
        if self.biz_attr.field is not None:
            value, errors = self.biz_attr.field.process(value)
            if errors:
                # TODO: raise proper exception
                console.error(
                    message=(
                        f'validation error in setting '
                        f'ViewProperty {self.biz_attr.name}',
                    ),
                    data={
                        'object': str(self),
                        'errors': errors,
                    }
                )
                raise Exception('validation error')

        super().fset(source, value)

        for func in self.biz_attr.on_set:
            func(source, value)

    def fdel(self, source: 'BizObject'):
        value = super().fget(source)
        super().fdel(source)
        for func in self.biz_attr.on_del:
            func(source, value)
