from typing import Text, Dict, Callable, Type, Tuple

from pybiz.schema import Schema
from pybiz.util import is_sequence
from pybiz.util.loggers import console

from .biz_attribute import BizAttribute, BizAttributeProperty


class View(BizAttribute):
    def __init__(
        self,
        load: Callable,
        transform: Callable = None,
        schema: Schema = None,
        *args, **kwargs
    ):
        super().__init__(*args, **kwargs)
        self._load = load
        self._transform = transform
        self._schema = schema

    @property
    def order_key(self):
        return 10

    @property
    def category(self):
        return 'view'

    @property
    def schema(self) -> Schema:
        return self._schema

    def build_property(self) -> 'ViewProperty':
        return ViewProperty(self)

    def execute(self, caller: 'BizObject', *args, **kwargs) -> object:
        args = args or {}
        data = self._load(caller, *args, **kwargs)
        if self._transform is not None:
            data = self._transform(caller, data, *args, **kwargs)
        return data


class ViewProperty(BizAttributeProperty):

    @property
    def view(self):
        return self.biz_attr

    def fset(self, bizobj, value):
        if view.schema is not None:
            value, errors = view.schema.process(value)
            if errors:
                # TODO: raise proper exception
                console.error(
                    message=(
                        f'validation error in setting '
                        f'ViewProperty {view.name}',
                    ),
                    data={
                        'object': str(self),
                        'errors': errors,
                    }
                )
                raise Exception('validation error')
        super().fset(bizobj, value)
