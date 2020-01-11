import uuid

from typing import Text, Tuple, Callable, Type

from appyratus.schema import Schema, fields
from appyratus.schema.fields import *

from pybiz.util.misc_functions import get_class_name


class Id(fields.Field):
    """
    This is a special Field recognized internally by Pybiz. When an Application
    bootstraps, all Id fields declared on BizObject classes are replaced with a
    concrete Field class.
    """

    def __init__(self, target: Type['BizObject'] = None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        if isinstance(target, type):
            self.target_biz_class_callback = None
            self.target_biz_class = target
        elif target is None:
            self.target_biz_class_callback = None
            self.target_biz_class = None

    def process(self, value):
        raise NotImplementedError()

    def __repr__(self):
        return (
            f'Id()'
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


fields.Id = Id
