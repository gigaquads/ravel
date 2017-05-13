import re
import pytz

from datetime import datetime, date
from abc import ABCMeta, abstractmethod

from .const import (
    RE_EMAIL, RE_UUID, RE_FLOAT,
    OP_LOAD, OP_DUMP,
    )


class ValidationError(Exception):
    def __init__(self, reasons: dict = None):
        self.reasons = reasons or {}
        super(ValidationError, self).__init__(str(self.reasons))


class Field(object, metaclass=ABCMeta):

    def __init__(self,
            allow_none=False,
            load_only=False,
            load_from=None,
            dump_to=None,
            required=False,
            ):

        self.load_only = load_only
        self.load_from = load_from
        self.dump_to = dump_to
        self.allow_none = allow_none
        self.required = required
        self.name = None

    def __repr__(self):
        return '<Field({}{})>'.format(
                self.__class__.__name__,
                ', name="{}"'.format(self.name) if self.name else '')

    @abstractmethod
    def load(self, data):
        pass

    @abstractmethod
    def dump(self, data):
        pass


class Nested(Field):

    def __init__(self, nested_schema, many=False, *args, **kwargs):
        super(Nested, self).__init__(*args, **kwargs)
        assert isinstance(nested_schema, Schema)
        self.nested_schema = nested_schema
        self.many = many

    def load(self, value):
        if not self.many:
            schema_result = self.nested_schema.load(value)
            if schema_result.errors:
                return FieldResult(error=schema_result.errors)
            else:
                return FieldResult(value=self.nested_schema.load(value).data)
        else:
            if not isinstance(value, (list, tuple, set)):
                return FieldResult(error='expected a valid sequence')
            result_list = []
            for i, x in enumerate(value):
                result = self.nested_schema.load(x)
                if result.errors:
                    return FieldResult(error={i: result.errors})
                result_list.append(result.data)
            return FieldResult(value=result_list)

    def dump(self, data):
        return self.load(data)


class List(Field):

    def __init__(self, nested_field, *args, **kwargs):
        super(List, self).__init__(*args, **kwargs)
        assert isinstance(nested_field, Field)
        self.nested_field = nested_field

    def load(self, value):
        if not isinstance(value, (list, tuple, set)):
            return FieldResult(error='expected a valid sequence')
        result_list = []
        for i, x in enumerate(value):
            result = self.nested_field.load(x)
            if result.error:
                return FieldResult(error={i: result.error})
            result_list.append(result.value)
        return FieldResult(value=result_list)

    def dump(self, data):
        return self.load(data)


class Str(Field):

    def load(self, value):
        if isinstance(value, str):
            return FieldResult(value)
        else:
            return FieldResult(error='expected a string')

    def dump(self, data):
        return self.load(data)


class Enum(Field):

    def __init__(self, nested, allowed_values, *args, **kwargs):
        super(Enum, self).__init__(*args, **kwargs)
        assert isinstance(nested, (Field, Schema))
        self.is_nested_field = isinstance(nested, Field)
        self.allowed_values = set(allowed_values)
        self.nested = nested

    def load(self, value):
        if value not in self.allowed_values:
            return FieldResult(error='unrecognized value')
        if self.is_nested_field:
            return self.nested.load(value)
        else:
            schema_result = self.nested.load(value)
            return FieldResult(
                    value=schema_result.data,
                    error=schema_result.errors)

    def dump(self, data):
        return self.load(data)


class Email(Field):

    def load(self, value):
        if isinstance(value, str):
            value = value.lower()
            if not RE_EMAIL.match(value):
                return FieldResult(error='not a valid e-mail address')
            return value
        else:
            return FieldResult(error='expected an e-mail address')

    def dump(self, data):
        return self.load(data)


class Uuid(Field):

    def load(self, value):
        if isinstance(value, UUID):
            return value.hex
        elif isinstance(value, str):
            value = value.replace('-', '').lower()
            if not RE_UUID.match(value):
                return FieldResult(error='invalid UUID')
        elif isinstance(value, int):
            hex_atr = hex(value)[2:]
            return ('0'*(32 - len(hex_str))) + hex_str
        else:
            return FieldResult(error='expected a UUID')

    def dump(self, data):
        return self.load(data)


class Int(Field):

    def load(self, value):
        if isinstance(value, int):
            return FieldResult(value=value)
        elif isinstance(value, str):
            if not value.isdigit():
                return FieldResult(error='expected an integer')
            return FieldResult(value=int(value))
        else:
            return FieldResult(error='expected an integer')

    def dump(self, data):
        return self.load(data)


class Float(Field):

    def load(self, value):
        if isinstance(value, float):
            return FieldResult(value=value)
        elif isinstance(value, str):
            if not RE_FLOAT.match(value):
                return FieldResult(error='expected a float')
            return FieldResult(value=float(value))
        else:
            return FieldResult(error='expected a float')

    def dump(self, data):
        return self.load(data)


class DateTime(Field):

    def load(self, value):
        if isinstance(value, (datetime, date)):
            return FieldResult(value=value.replace(tzinfo=pytz.utc))
        elif isinstance(value, (int, float)):
            try:
                return FieldResult(value=datetime.utcfromtimestamp(ts))
            except ValueError:
                return FieldResult(error='invalid UTC timestamp')
        elif isinstance(value, str):
            try:
                value = dateutil.parser.parse(value)
                return FieldResult(value=value)
            except ValueError:
                return FieldResult(error='unrecongized datetime string')
        else:
            return FieldResult(
                    error='expected a datetime string or timestamp')

    def dump(self, data):
        result = self.load(data)
        result.value = self.to_timestamp(result.value)
        return result

    @staticmethod
    def to_timestamp(datetime_obj):
        """
        Return the datetime object as a UTC timestamp in seconds.
        """
        if datetime_obj is None:
            return None
        if isinstance(datetime_obj, datetime):
            if datetime_obj.tzinfo is None:
                raise ValueError('datetime object has no timezone')
        elif isinstance(datetime_obj, date):
            datetime_obj = datetime\
                .strptime(str(datetime_obj), "%Y-%m-%d")\
                .replace(tzinfo=pytz.utc)
        epoch = datetime.fromtimestamp(0, pytz.utc)
        return int((datetime_obj - epoch).total_seconds())


class SchemaMeta(type):

    def __new__(cls, name, bases, dict_):
        return type.__new__(cls, name, bases, dict_)

    def __init__(cls, name, bases, dict_):
        type.__init__(cls, name, bases, dict_)

        cls.fields = {}
        cls.required_fields = {}
        cls.load_from_fields = {}
        for k, v in dict_.items():
            if isinstance(v, Field):
                v.name = k
                if v.load_from is not None:
                    cls.load_from_fields[v.load_from] = v
                cls.fields[k] = v
                if v.required:
                    cls.required_fields[k] = v


class Schema(object, metaclass=SchemaMeta):

    def __init__(self, strict=False):
        self.strict = strict

    def __repr__(self):
        return '<Schema({})>'.format(self.__class__.__name__)

    def load(self, data, strict=None):
        return self._transform(data, OP_LOAD, strict)

    def dump(self, data, strict=None):
        return self._transform(data, OP_DUMP, strict)

    def _transform(self, data, op, strict):
        assert op in (OP_LOAD, OP_DUMP)

        strict = strict if strict is not None else self.strict
        result = SchemaResult(op, {}, {})
        errors = {}

        for k, v in data.items():
            field = self.fields.get(k)

            if field is None:
                field = self.load_from_fields.get(k)
                if field is None:
                    result.errors[k] = 'unrecognized field'
                    continue

            if v is None:
                if not field.allow_none:
                    result.errors[k] = 'must not be null'
                    continue
                elif op == OP_DUMP and field.dump_to:
                    result.data[field.dump_to] = None
                elif op == OP_LOAD:
                    result.data[k] = None
                else:
                    result.data[k] = v
            else:
                field_result = getattr(field, op)(v)
                if field_result.error:
                    result.errors[k] = field_result.error
                elif op == OP_DUMP and field.dump_to:
                    result.data[field.dump_to] = field_result.value
                else:
                    result.data[field.name] = field_result.value

        for k in self.required_fields:
            if k not in result.data:
                if k not in result.errors:
                    result.errors[k] = 'required field'

        if strict and result.errors:
            result.raise_validation_error()

        return result


class SchemaResult(object):
    def __init__(self, op, data: dict, errors: dict):
        self.op = op
        self.data = data
        self.errors = errors

    def __repr__(self):
        return '<SchemaResult("{}", has_errors={})>'.format(
                self.op, True if self.errors else False)

    def raise_validation_error(self):
        raise ValidationError(reasons=self.errors)


class FieldResult(object):
    def __init__(self, value=None, error: str = None):
        self.value = value
        self.error = error

    def __repr__(self):
        return '<FieldResult(has_error={})>'.format(
                True if self.error else False)


if __name__ == '__main__':
    from pprint import pprint as pp

    class UserSchema(Schema):
        class NameSchema(Schema):
            first = Str()
            last = Str()

        created_at = DateTime()
        user_id = Int(load_from='id', dump_to='public_id')
        name = Nested(NameSchema())
        age = Int()
        rating = Float()
        sex = Enum(Str(), ('m', 'f', 'o'), required=True)
        race = Enum(Str(), ('white', 'asian', 'black'), required=True)
        friends = List(Str())


    schema = UserSchema()
    data = {
        'age': 5,
        'id': None,
        'rating': '5.2',
        'name': {'first': 'Bob', 'last': 999},
        'sex': 'm',
        'race': 'indian',
        'created_at': datetime.now(),
        'friends': ['Brian', 'KC', 5],
        }

    load_result = schema.load(data)
    dump_result = schema.dump(data)

    pp(load_result)
    pp(load_result.data)
    pp(load_result.errors)

    pp(dump_result)
    pp(dump_result.data)
    pp(dump_result.errors)
