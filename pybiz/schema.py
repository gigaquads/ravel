from typing import Text, Tuple, Callable, Type

from appyratus.schema import Schema, fields
from appyratus.schema.fields import *


class Id(fields.UuidString):
    """
    This is a special Field recognized internally by Pybiz. When an Application
    bootstraps, all Id fields declared on BizObject classes are replaced with a
    concrete Field class determined by the Applciation.id_field_class property.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.meta['pybiz_is_fk'] = True  # "is foriegn key"

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
    """
    Transformers are used by FieldPropertyQuery objects to perform various
    transformations on queried field values.
    """

    def transform(self, transform: Text, source, args: Tuple) -> object:
        func = getattr(self, transform, None)
        if func is not None:
            return func(source, *args)
        else:
            raise KeyError(f'no transform method: {transform}')


class StringTransformer(Transformer):
    """
    Transformers are used by FieldPropertyQuery objects to perform various
    transformations on queried String field values.
    """

    def lower(self, source: Text, is_set, *args: Tuple) -> Text:
        return source.lower() if is_set else source

    def upper(self, source: Text, is_set, *args: Tuple) -> Text:
        return source.upper() if is_set else source
