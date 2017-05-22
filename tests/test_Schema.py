import pytest
import mock

from pybiz import schema as fields
from pybiz.schema import Schema, ValidationError

# TODO: test Schema dump error
# TODO: test Schema load ok/error

@pytest.fixture(scope='module')
def MySchema():
    class MySchema(Schema):
        my_str = fields.Str()
        my_int = fields.Int()
    return MySchema


@pytest.fixture(scope='module')
def MySchemaRequired():
    class MySchemaRequired(Schema):
        my_str = fields.Str(required=True)
        my_int = fields.Int(required=True)
    return MySchemaRequired


@pytest.fixture(scope='module')
def MySchemaDumpTo():
    class MySchemaDumpTo(Schema):
        my_str = fields.Str(dump_to='my_str_dumped')
    return MySchemaDumpTo


@pytest.fixture(scope='module')
def MySchemaLoadFrom():
    class MySchemaDumpTo(Schema):
        my_str = fields.Str(load_from='my_str_field')
    return MySchemaDumpTo


@pytest.fixture(scope='module')
def MySchemaAllowNone():
    class MySchemaAllowNone(Schema):
        my_str_1 = fields.Str(allow_none=True)
        my_str_2 = fields.Str(allow_none=False)
    return MySchemaAllowNone


def test_Schema_fields(MySchema):
    schema = MySchema()
    schema.my_str = mock.MagicMock()
    schema.my_int = mock.MagicMock()
    data = {'my_int': 1, 'my_str': 'x'}
    schema.load(data)
    assert schema.my_str.load.called_once_with('x')
    assert schema.my_int.load.called_once_with('1')


@pytest.mark.parametrize('input_val, exp_retval', [
    ({'my_str': 'Saman', 'my_int': 34},
     {'my_str': 'Saman', 'my_int': 34}),
    ({'my_str': 'Saman', 'my_int': 34, 'foo': 'bar'},
     {'my_str': 'Saman', 'my_int': 34}),
    ({'my_str': 'Saman'},
     {'my_str': 'Saman'}),
    ({},
     {}),
    ])
def test_Schema_dump_ok(MySchema, input_val, exp_retval):
    schema = MySchema()
    result = schema.dump(input_val)
    assert not result.errors
    assert result.data == exp_retval


def test_Schema_allow_additional(MySchema):
    input_val = {'my_int': 1, 'my_str': 'a', 'foo': 'bar'}

    schema = MySchema(allow_additional=True)
    result = schema.load(input_val)
    assert result.errors == {}

    schema = MySchema(allow_additional=False)
    result = schema.load(input_val)
    assert result.errors.keys() == {'foo'}
    assert result.errors['foo'] == 'unrecognized field'


def test_Schema_strict(MySchema):
    input_val = {'my_int': 'foo', 'my_str': 'a'}
    schema = MySchema()
    with pytest.raises(ValidationError):
        schema.load(input_val, strict=True)


def test_Schema_required(MySchemaRequired):
    input_val = {'my_int': 1}
    schema = MySchemaRequired()
    result = schema.load(input_val)
    assert 'my_str' in result.errors
    assert result.errors['my_str'] == 'required field'


def test_Schema_dump_to(MySchemaDumpTo):
    input_val = {'my_str': 'foo'}
    schema = MySchemaDumpTo()
    result = schema.dump(input_val)
    assert result.data.get('my_str_dumped') == 'foo'


def test_Schema_load_from(MySchemaLoadFrom):
    input_val = {'my_str_field': 'foo'}
    schema = MySchemaLoadFrom()
    result = schema.load(input_val)
    assert result.data.get('my_str') == 'foo'

    input_val = {'my_str': 'foo'}
    result = schema.load(input_val)
    assert result.data.get('my_str') == 'foo'


def test_Schema_allow_none(MySchemaAllowNone):
    schema = MySchemaAllowNone()

    input_val = {'my_str_1': None, 'my_str_2': 'y'}
    result = schema.load(input_val)
    assert result.data.get('my_str_1') is None
    assert not result.errors

    input_val = {'my_str_1': 'x', 'my_str_2': None}
    result = schema.load(input_val)
    assert result.data.get('my_str_1') is 'x'
    assert 'my_str_2' in result.errors
