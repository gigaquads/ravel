from typing import Dict, Text, Type

from ravel.util import get_class_name
from ravel.util.json_encoder import JsonEncoder
from ravel.schema import Schema, fields as field_types

STRING_FIELD_TYPE_2_FORMAT = {
    field_types.UuidString: 'uuid',
    field_types.Email: 'email',
    field_types.Url: 'uri',
    field_types.DateTimeString: 'date-time',
}


class JsonSchemaGenerator:
    def __init__(self, hostname: Text = None):
        self.hostname = (hostname or '').rstrip('/')
        self.json = JsonEncoder()

    def from_schema(self, schema: 'Schema', encode=False) -> Dict:
        schema_class_name = self.get_schema_name(schema)
        json_schema = {
            '$schema': 'http://json-schema.org/schema#',
            '$id': f'{self.hostname}/schemas/{schema_class_name}.json',
            'properties': self._derive_properties(schema),
            'required': list(schema.required_fields.keys()),
        }
        if encode:
            return self.json.encode(json_schema)
        return json_schema

    def from_resource(self, resource_type: Type['Resource'], encode=False) -> Dict:
        json_schema = self.from_schema(resource_type.ravel.schema)
        for resolver in resource_type.ravel.resolvers.values():
            if resolver.private:
                continue
            if resolver.name not in resource_type.ravel.schema.fields:
                target_schema = resolver.target.ravel.schema
                field_dict = self._derive_property(target_schema)
                if resolver.many:
                    json_schema['properties'][resolver.name] = {
                        'type': 'array',
                        'default': [],
                        'items': field_dict
                    }
                else:
                    json_schema[resolver.name] = field_dict
        if encode:
            return self.json.encode(json_schema)
        return json_schema

    def _derive_properties(self, schema):
        properties = {}
        for field in schema.fields.values():
            if not field.meta.get('private', False):
                prop = self._derive_property(field)
                properties[field.name] = prop
        return properties

    def _derive_property(self, field: 'Field'):
        field_dict = {}
        if isinstance(field, field_types.String):
            field_dict['type'] = 'string'
            format_str = STRING_FIELD_TYPE_2_FORMAT.get(type(field))
            if format_str is not None:
                field_dict['format'] = format_str
        elif isinstance(field, field_types.Bool):
            field_dict['type'] = 'boolean'
        elif isinstance(field, field_types.Int):
            field_dict['type'] = 'integer'
        elif isinstance(field, field_types.Float):
            field_dict['type'] = 'number'
        elif isinstance(field, field_types.DateTime):
            field_dict['type'] = 'integer'
            field_dict['format'] = 'date-time'
        elif isinstance(field, field_types.DateTimeString):
            field_dict['type'] = 'string'
            field_dict['format'] = 'date-time'
        elif isinstance(field, field_types.Dict):
            field_dict['type'] = 'object'
        elif isinstance(field, (field_types.List, field_types.Set)):
            field_dict['type'] = 'array'
            field_dict['items'] = self._derive_property(field.nested)
            field_dict['default'] = []
        elif isinstance(field, field_types.Enum):
            inner_field_dict = self._derive_property(field.nested)
            field_dict['type'] = inner_field_dict['type']
            field_dict['enum'] = list(field.values)
        elif isinstance(field, Schema):
            defined_name = self.get_schema_name(field)
            field_dict['$ref'] = defined_name
        elif isinstance(field, field_types.Nested):
            defined_name = self.get_schema_name(field.schema_type)
            field_dict['$ref'] = defined_name
        elif isinstance(field, field_types.Uuid):
            field_dict['type'] = 'string'
            field_dict['format'] = 'uuid'
        else:
            field_dict['type'] = 'null'

        if field.nullable:
            field_dict['type'] = [field_dict['type'], 'null']

        return field_dict

    @staticmethod
    def get_schema_name(schema: 'Schema') -> Text:
        name = get_class_name(schema)
        if name.endswith('Schema'):
            name = name[:-6]
        return name

