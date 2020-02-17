import uuid

from typing import Text, Tuple, Callable, Type

from appyratus.schema import Schema, fields
from appyratus.schema.fields import *

from ravel.util.misc_functions import get_class_name


class Id(fields.Field):
    """
    This is a special Field recognized internally by Pybiz. When an Application
    bootstraps, all Id fields declared on Resource classes are replaced with a
    concrete Field class.
    """

    def __init__(self, target: Type['Resource'] = None, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.target_resource_type_name = None
        self.target_resource_type_callback = None
        self.target_resource_type = None

        if isinstance(target, type):
            self.target_resource_type = target
        elif isinstance(target, str):
            self.target_resource_type_name = target
        elif callable(target):
            self.target_resource_type_callback = target

    def resolve_target_resource_type(self, app: 'Application') -> Type['Resource']:
        if self.target_resource_type is None:
            if self.target_resource_type_callback is not None:
                app.inject(self.target_resource_type_callback)
                self.target_resource_type = self.target_resource_type_callback()
            else:
                self.target_resource_type = app.biz[self.target_resource_type_name]
        return self.target_resource_type

    # TODO: rename replace_self_in_resource_type
    def replace_self_in_resource_type(
        self,
        app: 'Application',
        source_resource_type: Type['Resource'],
    ):
        # compute the replacement field to use in place of Id
        target_resource_type = self.resolve_target_resource_type(app)
        target_id_field_type = type(target_resource_type._id.resolver.field)
        replacement_field = target_id_field_type(
            name=self.name,
            source=self.source,
            required=self.required,
            meta=self.meta,
        )
        # dynamically replace the existing field in Schema class
        source_resource_type.Schema.replace_field(replacement_field)

        # update existing FieldResolver to use the replacement field
        field_resolver = source_resource_type.ravel.resolvers.fields[self.name]
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
