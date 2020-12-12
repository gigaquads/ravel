import uuid

from typing import Text, Tuple, Callable, Type

from appyratus.schema import Schema, Field, fields

from ravel.util.misc_functions import get_class_name

# TODO: in Resource meta class, recurse through nested schemas for Id fields
#       for resolving them latar

class Id(fields.Field):
    """
    This is a special Field recognized internally by Ravel. When an Application
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
                self.target_resource_type = app[self.target_resource_type_name]
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
            nullable=self.nullable,
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
        return ('Id()')


# for import convenience:
fields.Id = Id