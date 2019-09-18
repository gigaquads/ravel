from typing import Text, Tuple, Callable, Type

from appyratus.schema import Schema, fields
from appyratus.schema.fields import *


class Id(fields.UuidString):
    def replace_with(self, replacement_field_class: Type[Field]):
        """
        This is used internally when Pybiz replaces all Id fields declared on
        BizObject classes with the custom Field type specified by the host
        Pybiz Application, via the `id_field_class` property.
        """
        return replacement_field_class(
            name=self.name,
            source=self.source,
            required=self.required,
            nullable=self.nullable,
            default=self.default,
            meta=self.meta
        )


class Transformer(object):
    def transform(self, transform: Text, source, args: Tuple) -> object:
        func = getattr(self, transform, None)
        if func is not None:
            return func(source, *args)
        else:
            raise KeyError(f'no transform method: {transform}')


class StringTransformer(Transformer):
    def lower(self, source: Text, is_set, *args: Tuple) -> Text:
        return source.lower() if is_set else source

    def upper(self, source: Text, is_set, *args: Tuple) -> Text:
        return source.upper() if is_set else source
