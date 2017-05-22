import pytest
import mock

from uuid import UUID

from pybiz import schema as fields
from pybiz.schema import Schema

# TODO: List dump ok/error
# TODO: Enum dump, load ok/error
# TODO: Nested dump, load ok/error


@pytest.mark.parametrize(
    'Field, input_vals, exp_retval', [
        (fields.Str, ['x'], 'x'),
        (fields.Int, [1, '1'], 1),
        (fields.Float, [1, 1.0, '1.', '1.0'], 1.0),
        (fields.Float, ['.1'], 0.1),
        (fields.Email, ['a@b.com', 'A@B.COM'], 'a@b.com'),
        (fields.Email, ['a.d@b.com'], 'a.d@b.com'),
        (fields.Uuid, [UUID('1'*32), '1'*32, str(UUID('1'*32))], '1'*32),
        (fields.Uuid, [1], '0'*31 + '1'),
        ])
def test_field_load_ok(Field, input_vals, exp_retval):
    field = Field()
    for val in input_vals:
        y = field.load(val)
        assert not y.error
        assert exp_retval == y.value


@pytest.mark.parametrize(
    'Field, bad_input_vals', [
        (fields.Str, [1, None]),
        (fields.Int, ['x', None]),
        (fields.Float, ['x', None]),
        (fields.Email, ['.d@b.com', 'a@b.', 'abc', None, '']),
        (fields.Uuid, ['', 'z'*32, 'a', None]),
        ])
def test_field_load_error(Field, bad_input_vals):
    field = Field()
    for bad_val in bad_input_vals:
        y = field.load(bad_val)
        assert y.error
        assert y.value is None


@pytest.mark.parametrize(
    'Field, input_vals, exp_retval', [
        (fields.Str, ['x'], 'x'),
        (fields.Int, [1, '1'], 1),
        (fields.Float, [1, '1', 1.0, '1.', '1.0'], 1.0),
        (fields.Float, ['.1'], 0.1),
        (fields.Uuid, [UUID('1'*32), '1'*32, str(UUID('1'*32))], '1'*32),
        (fields.Uuid, [1], '0'*31 + '1'),
        ])
def test_field_dump_ok(Field, input_vals, exp_retval):
    field = Field()
    for val in input_vals:
        y = field.dump(val)
        assert not y.error
        assert exp_retval == y.value


@pytest.mark.parametrize(
    'Field, bad_input_vals', [
        (fields.Str, [1, None]),
        (fields.Int, ['x', None]),
        (fields.Float, ['x', None]),
        (fields.Email, ['.d@b.com', 'a@b.', 'abc', None, '']),
        (fields.Uuid, ['', 'z'*32, 'a', None]),
        ])
def test_field_dump_error(Field, bad_input_vals):
    field = Field()
    for bad_val in bad_input_vals:
        y = field.dump(bad_val)
        assert y.error
        assert y.value is None


@pytest.mark.parametrize(
    'data', [
        ['x', 'y', 'z'],
        [],
        (),
        set(),
        ])
def test_List_load_ok(data):
    mock_str_field = mock.MagicMock()
    field = fields.List(mock_str_field)
    field.load(data)
    for x in data:
        assert field.nested_field.load.called_once_with(x)


def test_List_load_empty_list_ok():
    input_list = []
    field = fields.List(fields.Str())
    retval = field.load(input_list)
    assert retval.value == []


@pytest.mark.parametrize(
    'data', [
        ['x', 1, 'z'],
        None,
        ])
def test_List_load_error(data):
    field = fields.List(fields.Str())
    retval = field.load(data)
    assert retval.error
    assert retval.value is None
