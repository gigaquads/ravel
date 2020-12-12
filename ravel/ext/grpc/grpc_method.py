import inspect 

from typing import Text, List, Set, Dict, ByteString

from appyratus.schema import Schema, fields as field_types
from appyratus.utils.dict_utils import DictObject
from appyratus.utils.string_utils import StringUtils

from ravel.app import Action
from ravel.util.misc_functions import extract_res_info_from_annotation
from ravel.util import is_resource_type, get_class_name
from ravel.util.loggers import console
from ravel.constants import ID, REV

from .util import get_stripped_schema_name
from .proto import MessageGenerator


class GrpcMethod(Action):

    _py_type_2_field_type = {
        int: field_types.Int,
        float: field_types.Float,
        str: field_types.String,
        Text: field_types.String,
        bool: field_types.Bool,
        bytes: field_types.Bytes,
        ByteString: field_types.Bytes,
        dict: field_types.Dict,
        Dict: field_types.Dict,
        list: field_types.List,
        List: field_types.List,
        set: field_types.Set,
        Set: field_types.Set,
    }

    def __init__(self, target, decorator):
        super().__init__(target, decorator)
        self._msg_gen = MessageGenerator()
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
        text = self._msg_gen.emit(self.app, self.schemas.request)
        return text + '\n' if text is not None else None

    def generate_response_message_type(self) -> Text:
        text = self._msg_gen.emit(self.app, self.schemas.response)
        return text + '\n' if text is not None else None

    def generate_protobuf_function_declaration(self, stream=None) -> Text:
        request_type_name = get_stripped_schema_name(self.schemas.request)
        response_type_name = get_stripped_schema_name(self.schemas.response)

        if stream is not None:
            stream_str = 'stream ' if stream else ''
        elif self._returns_stream:
            stream_str = 'stream '
        else:
            stream_str = ''

        return (
            f'rpc {self.name}({request_type_name}) '
            f'returns ({stream_str}{response_type_name})'
            f' {{}}'
        )

    def _build_response_schema(self):
        type_name = f'{StringUtils.camel(self.name)}ResponseSchema'
        obj = self.decorator.kwargs.get('response')

        if isinstance(obj, dict):
            schema = Schema.factory(type_name, obj)()
        elif isinstance(obj, Schema):
            response_schema_type = type(type_name, (type(obj), ), {})
            schema = response_schema_type()
        elif is_resource_type(obj):
            response_schema_type = type(type_name, (obj.Schema, ), {})
            schema = response_schema_type()
        else:
            many, resource_type_name = extract_res_info_from_annotation(
                self.signature.return_annotation
            )
            self._returns_stream = many
            if resource_type_name in self.app.manifest.resource_classes:
                base_schema_type = self.app[resource_type_name].Schema
                response_schema_type = type(type_name, (base_schema_type, ), {})
                schema = response_schema_type()
            else:
                schema = Schema.factory(type_name, {})()

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
        type_name = f'{StringUtils.camel(self.name)}RequestSchema'
        obj = self.decorator.kwargs.get('request')

        if isinstance(obj, dict):
            schema = Schema.factory(type_name, obj)()
        elif isinstance(obj, Schema):
            request_schema_type = type(type_name, (type(obj), ), {})
            schema = request_schema_type()
        elif is_resource_type(obj):
            request_schema_type = type(type_name, (obj.Schema, ), {})
            schema = request_schema_type()
        else:
            fields = self._infer_request_fields()
            schema = Schema.factory(type_name, fields)()

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

        if type_name in self.app.manifest.resource_classes:
            if many:
                field = field_types.List(self.app[type_name].Schema())
            else:
                field = field_types.Nested(self.app[type_name].Schema())
        elif annotation is not None:
            field_type = self._py_type_2_field_type.get(annotation)
            if field_type:
                if many:
                    field = field_types.List(field_type())
                else:
                    field = field_type()
            elif annotation is not inspect._empty:
                console.error(
                    message=f'cannot infer protobuf field from annotation',
                    data={
                        'annotation': str(annotation),
                        'name': name,
                    }
                )

        if field is not None and name:
            field.name = name

        return field
