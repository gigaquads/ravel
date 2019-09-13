from typing import Text, Tuple, Callable

from appyratus.schema import Schema, fields
from appyratus.schema.fields import *


class Id(fields.UuidString):
    pass


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
