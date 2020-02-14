import uuid

from typing import Text, Tuple, Callable, Type

from appyratus.schema import Schema, fields
from appyratus.schema.fields import *

from pybiz.util.misc_functions import get_class_name


class Id(fields.Field):
    """
    This is a special Field recognized internally by Pybiz. When an Application
    bootstraps, all Id fields declared on Resource classes are replaced with a
    concrete Field class.
    """

    def __init__(self, target: Type['Resource'] = None, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.target_biz_class_name = None
        self.target_biz_class_callback = None
        self.target_biz_class = None

        if isinstance(target, type):
            self.target_biz_class = target
        elif isinstance(target, str):
            self.target_biz_class_name = target
        elif callable(target):
            self.target_biz_class_callback = target

    def resolve_target_biz_class(self, app: 'Application') -> Type['Resource']:
        if self.target_biz_class is None:
            if self.target_biz_class_callback is not None:
                app.inject(self.target_biz_class_callback)
                self.target_biz_class = self.target_biz_class_callback()
            else:
                self.target_biz_class = app.biz[self.target_biz_class_name]
        return self.target_biz_class

    # TODO: rename replace_self_in_biz_class
    def replace_self_in_biz_class(
        self,
        app: 'Application',
        source_biz_class: Type['Resource'],
    ):
        # compute the replacement field to use in place of Id
        target_biz_class = self.resolve_target_biz_class(app)
        target_id_field_type = type(target_biz_class._id.resolver.field)
        replacement_field = target_id_field_type(
            name=self.name,
            source=self.source,
            required=self.required,
            meta=self.meta,
        )
        # dynamically replace the existing field in Schema class
        source_biz_class.Schema.replace_field(replacement_field)

        # update existing FieldResolver to use the replacement field
        field_resolver = source_biz_class.pybiz.resolvers.fields[self.name]
        field_resolver.field = replacement_field

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
