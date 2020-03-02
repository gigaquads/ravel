from typing import Text, List

from appyratus.schema import Schema, fields as field_types
from appyratus.utils import DictObject, StringUtils

from ravel.app import Action
from ravel.util.misc_functions import extract_res_info_from_annotation
from ravel.util import is_resource_type, get_class_name
from ravel.constants import ID, REV

from .proto import MessageGenerator


class GrpcFunction(Action):

    _py_type_2_field_type = {
        'int': field_types.Int,
        'float': field_types.Float,
        'str': field_types.String,
        'bool': field_types.Bool,
        'bytes': field_types.Bytes,
        'dict': field_types.Dict,
        'list': field_types.List,
        'set': field_types.Set,
    }

    def __init__(self, target, decorator):
        super().__init__(target, decorator)
        self._msg_gen = MessageGenerator()
        self._msg_name_prefix = None
        self._returns_stream = False
        self.schemas = DictObject()

    def __call__(self, *raw_args, **raw_kwargs):
        return super().__call__(*(raw_args[:1]), **raw_kwargs)

    def on_bootstrap(self):
        self._msg_name_prefix = StringUtils.camel(self.name)
        self.schemas.request = self._build_request_schema()
        self.schemas.response = self._build_response_schema()

    @property
    def streams_response(self) -> bool:
        return self._returns_stream

    def generate_request_message_type(self) -> Text:
        return self._msg_gen.emit(self.schemas.request) + '\n'

    def generate_response_message_type(self) -> Text:
        return self._msg_gen.emit(self.schemas.response) + '\n'

    def generate_protobuf_function_declaration(self, stream=None) -> Text:
        req_msg_type = get_class_name(self.schemas.request)
        resp_msg_type = get_class_name(self.schemas.response)
        if stream is not None:
            stream_str = 'stream ' if stream else ''
        elif self._returns_stream:
            stream_str = 'stream '
        else:
            stream_str = ''

        return (
            f'rpc {self.name}({req_msg_type}) '
            f'returns ({stream_str}{resp_msg_type})'
            f' {{}}'
        )

    def _build_response_schema(self):
        default_type_name = f'{self._msg_name_prefix}Response'
        obj = self.decorator.kwargs.get('response')

        if isinstance(obj, dict):
            schema = Schema.factory(default_type_name, kwarg)()
        elif isinstance(obj, Schema):
            schema = obj
        elif is_resource_type(obj):
            schema = obj.Schema()
        else:
            many, type_name = extract_res_info_from_annotation(
                self.signature.return_annotation
            )
            self._returns_stream = many
            if type_name in self.app.res:
                schema = self.app.res[type_name].Schema()
            else:
                schema = Schema.factory(default_type_name, {})()

        schema.name = StringUtils.snake(get_class_name(schema))
        self._insert_field_numbers(schema)
        return schema

    def _insert_field_numbers(self, schema):
        counter = 1
        if ID in schema.fields:
            schema.fields[ID].meta['field_no'] = counter
            counter += 1
        if REV in schema.fields:
            schema.fields[REV].meta['field_no'] = counter
            counter += 1
        for f in schema.fields.values():
            if f.name not in {ID, REV}:
                f.meta['field_no'] = counter
                counter += 1

    def _build_request_schema(self):
        default_type_name = f'{self._msg_name_prefix}Request'
        obj = self.decorator.kwargs.get('request')

        if isinstance(obj, dict):
            schema = Schema.factory(default_type_name, kwarg)()
        elif isinstance(obj, Schema):
            schema = obj
        elif is_resource_type(obj):
            schema = obj.Schema()
        else:
            fields = self._infer_request_fields()
            schema = Schema.factory(default_type_name, fields)()

        schema.name = StringUtils.snake(get_class_name(schema))
        self._insert_field_numbers(schema)
        return schema

    def _infer_request_fields(self):
        fields = {}
        for param in self.signature.parameters.values():
            field = self._infer_field(param.annotation, name=param.name)
            if field is not None:
                fields[param.name] = field
        return fields

    def _infer_field(self, annotation, name=None):
        many, type_name = extract_res_info_from_annotation(annotation)
        field = None

        if type_name in self.app.res:
            if many:
                field = field_types.List(self.app.res[type_name].Schema())
            else:
                field = field_types.Nested(self.app.res[type_name].Schema())
        elif type_name is not None:
            field_type = self._py_type_2_field_type.get(type_name)
            if many:
                field = field_types.List(field_type())
            else:
                field = field_type()

        if field is not None and name:
            field.name = name

        return field
