from appyratus.schema import Schema
from appyratus.schema.fields import Field


class FieldAdapter(object):
    def __init__(self):
        self.msg_gen = None

    def emit(self, field: Field, field_no: int):
        raise NotImplementedError()

    def bind(self, msg_gen: 'MessageGenerator'):
        self.msg_gen = msg_gen

    def emit(self, field_type, field_no, field_name, is_repeated=False):
        return '{repeated} {field_type} {field_name}{field_no}'.format(
            repeated=' repeated' if is_repeated else '',
            field_type=field_type,
            field_name=field_name,
            field_no=f' = {field_no}',
        )


class ScalarFieldAdapter(FieldAdapter):
    def __init__(self, type_name):
        self.type_name = type_name

    def emit(self, field, field_no):
        field_type = self.msg_gen.get_adapter(field).type_name
        return super().emit(
            field_type=field_type,
            field_no=field_no,
            field_name=field.name
        )


class ArrayFieldAdapter(FieldAdapter):
    def emit(self, field, field_no):
        if isinstance(field.nested, Schema):
            nested_field_type = field.nested.__class__.__name__
        else:
            adapter = self.msg_gen.get_adapter(field.nested)
            try:
                if isinstance(adapter, SchemaFieldAdapter):
                    nested_field_type = field.nested.__class__.__name__
                elif isinstance(adapter, NestedFieldAdapter):
                    nested_field_type = field.nested.schema_type.__name__
                else:
                    nested_field_type = adapter.type_name
            except:
                raise Exception('Unable to establish nested field type')

        return super().emit(
            field_type=nested_field_type,
            field_no=field_no,
            field_name=field.name,
            is_repeated=True,
        )


class NestedFieldAdapter(FieldAdapter):
    def emit(self, field, field_no):
        return super().emit(
            field_type=field.schema_type.__name__,
            field_no=field_no,
            field_name=field.name,
        )


class SchemaFieldAdapter(FieldAdapter):
    def emit(self, field, field_no):
        return super().emit(
            field_type=field.__class__.__name__,
            field_no=field_no,
            field_name=field.name,
        )


class EnumFieldAdapter(FieldAdapter):
    def emit(self, field, field_no):
        nested_field_type = self.msg_gen.get_adapter(field.nested).type_name
        return super().emit(
            field_type=nested_field_type,
            field_no=field_no,
            field_name=field.name,
        )
