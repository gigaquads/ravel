import pytest

from mock import MagicMock

from pybiz.const import (
    ROOT_ATTR,
    OP_DELTA_ADD,
    OP_DELTA_REMOVE,
    OP_DELTA_REPLACE,
    )
from pybiz.patch import (
    JsonPatchPathComponent,
    JsonPatchMixin,
    JsonPatchError,
    )


@pytest.fixture(scope='function')
def mock_bizobj():
    bizobj = MagicMock()
    bizobj.is_bizobj = True
    bizobj._parse_path = lambda path: JsonPatchMixin._parse_path(bizobj, path)
    bizobj.a = {'d': [1, 2]}
    bizobj.b.is_bizobj = True
    bizobj.b.c = 'c'
    bizobj.my_dict_list = [{}, {}]
    bizobj.my_dict_list_2 = [{'ints': [3, 4]}, {}]
    return bizobj


@pytest.mark.parametrize('path, exp_tokenized_path', [
    ('/a', [ROOT_ATTR, 'a']),
    ('/a/', [ROOT_ATTR, 'a']),
    ('/a/b', [ROOT_ATTR, 'a', 'b']),
    ('a/b', [ROOT_ATTR, 'a', 'b']),
    ('/a//b', [ROOT_ATTR, 'a', 'b']),
    ('/a/b/1', [ROOT_ATTR, 'a', 'b', '1']),
    ])
def test_JsonPatchMixin_parse_path(path, exp_tokenized_path, mock_bizobj):
    tokenized_path = JsonPatchMixin._parse_path(mock_bizobj, path)
    assert tokenized_path == exp_tokenized_path


@pytest.mark.parametrize('path', ['/', ''])
def test_JsonPatchMixin_parse_path_error(mock_bizobj, path):
    with pytest.raises(JsonPatchError):
        JsonPatchMixin._parse_path(mock_bizobj, path)


def test_JsonPatchMixin_build_match_context_1(mock_bizobj):
    path = '/a'
    exp_objs = [mock_bizobj, mock_bizobj.a]
    exp_bizobj_list = [(0, mock_bizobj)]
    _test_JsonPatchMixin_build_match_context(
            mock_bizobj, path, exp_bizobj_list, exp_objs)


def test_JsonPatchMixin_build_match_context_3(mock_bizobj):
    path = '/b'
    exp_objs = [mock_bizobj, mock_bizobj.b]
    exp_bizobj_list = [(0, mock_bizobj), (1, mock_bizobj.b)]
    _test_JsonPatchMixin_build_match_context(
            mock_bizobj, path, exp_bizobj_list, exp_objs)


def test_JsonPatchMixin_build_match_context_4(mock_bizobj):
    path = '/b/c'
    exp_objs = [mock_bizobj, mock_bizobj.b, mock_bizobj.b.c]
    exp_bizobj_list = [(0, mock_bizobj), (1, mock_bizobj.b)]
    _test_JsonPatchMixin_build_match_context(
            mock_bizobj, path, exp_bizobj_list, exp_objs)


def test_JsonPatchMixin_build_match_context_5(mock_bizobj):
    path = '/a/d'
    exp_objs = [mock_bizobj, mock_bizobj.a, mock_bizobj.a['d']]
    exp_bizobj_list = [(0, mock_bizobj)]
    _test_JsonPatchMixin_build_match_context(
            mock_bizobj, path, exp_bizobj_list, exp_objs)


def test_JsonPatchMixin_build_match_context_6(mock_bizobj):
    for i in range(len(mock_bizobj.a['d'])):
        path = '/a/d/{}'.format(i)
        exp_objs = [
                mock_bizobj, mock_bizobj.a,
                mock_bizobj.a['d'], mock_bizobj.a['d'][i]]
        exp_bizobj_list = [(0, mock_bizobj)]
        _test_JsonPatchMixin_build_match_context(
                mock_bizobj, path, exp_bizobj_list, exp_objs)


def _test_JsonPatchMixin_build_match_context(
        mock_bizobj, path, exp_bizobj_list, exp_objs):
    ctx = JsonPatchMixin._build_patch_context(mock_bizobj, '', path)
    assert ctx['path'] == path
    assert 'tokenized_path' in ctx
    assert len(ctx['tokenized_path']) >= 1
    assert ctx['bizobjs'] == exp_bizobj_list
    assert ctx['objs'] == exp_objs


def test_JsonPatchMixin_build_relative_path(mock_bizobj):
    ctx = {
        'path': '/',
        'tokenized_path': ['/'],
        'objs': [mock_bizobj],
        }
    idx = 0
    path = JsonPatchMixin._build_relative_path(ctx, idx)
    assert len(path) == 1
    assert path[0][0] == path[0].key_in_parent == '/'
    assert path[0][1] == path[0].obj == mock_bizobj

    ctx = {
        'path': '/a',
        'tokenized_path': ['/', 'a'],
        'objs': [mock_bizobj, mock_bizobj.a],
        }
    idx = 1
    path = JsonPatchMixin._build_relative_path(ctx, idx)
    assert len(path) == 1
    assert path[0][0] == path[0].key_in_parent == 'a'
    assert path[0][1] == path[0].obj == mock_bizobj.a

    idx = 0
    path = JsonPatchMixin._build_relative_path(ctx, idx)
    assert len(path) == 2
    assert path[0][0] == path[0].key_in_parent == '/'
    assert path[0][1] == path[0].obj == mock_bizobj
    assert path[1][0] == path[1].key_in_parent == 'a'
    assert path[1][1] == path[1].obj == mock_bizobj.a

    ctx = {
        'path': '/b/c',
        'tokenized_path': ['/', 'b', 'c'],
        'objs': [mock_bizobj, mock_bizobj.b, mock_bizobj.b.c],
        }
    idx = 0
    path = JsonPatchMixin._build_relative_path(ctx, idx)
    assert len(path) == 3
    assert path[0][0] == path[0].key_in_parent == '/'
    assert path[0][1] == path[0].obj == mock_bizobj
    assert path[1][0] == path[1].key_in_parent == 'b'
    assert path[1][1] == path[1].obj == mock_bizobj.b
    assert path[2][0] == path[2].key_in_parent == 'c'
    assert path[2][1] == path[2].obj == mock_bizobj.b.c


def test_JsonPatchMixin_apply_delta_add_to_dict_1(mock_bizobj):
    op = OP_DELTA_ADD
    value = 1
    path = [
        JsonPatchPathComponent('/', mock_bizobj),
        JsonPatchPathComponent('a', mock_bizobj.a),
        JsonPatchPathComponent('c', value),
        ]

    assert mock_bizobj.a == {'d': [1, 2]}
    JsonPatchMixin.apply_delta(mock_bizobj, op, path, value=value)
    assert mock_bizobj.a == {'c': 1, 'd': [1, 2]}


def test_JsonPatchMixin_apply_delta_add_to_dict_2(mock_bizobj):
    op = OP_DELTA_ADD
    value = 'y'
    path = [
        JsonPatchPathComponent('/', mock_bizobj),
        JsonPatchPathComponent('my_dict_list', mock_bizobj.my_dict_list),
        JsonPatchPathComponent('1', mock_bizobj.my_dict_list[1]),
        JsonPatchPathComponent('x', None),
        ]

    assert mock_bizobj.my_dict_list == [{}, {}]
    JsonPatchMixin.apply_delta(mock_bizobj, op, path, value=value)
    assert mock_bizobj.my_dict_list == [{}, {'x': 'y'}]
    """
    mock_bizobj._build_relative_path = JsonPatchMixin._build_relative_path
    mock_bizobj._build_patch_context = (lambda *args:
            JsonPatchMixin._build_patch_context(mock_bizobj, *args))
    mock_bizobj._apply_patch_delta = (lambda *args:
            JsonPatchMixin._apply_patch_delta(mock_bizobj, *args))
    mock_bizobj.apply_delta = (lambda *args:
            JsonPatchMixin.apply_delta(mock_bizobj, *args))
    mock_bizobj.get_patch_hook.return_value = None

    JsonPatchMixin.patch(mock_bizobj, 'add', '/my_dict_list/1/z', 'w')
    """


def test_JsonPatchMixin_apply_delta_add_to_list_1(mock_bizobj):
    op = OP_DELTA_ADD
    value = 3
    path = [
        JsonPatchPathComponent('/', mock_bizobj),
        JsonPatchPathComponent('a', mock_bizobj.a),
        JsonPatchPathComponent('d', mock_bizobj.a['d']),
        ]

    assert mock_bizobj.a == {'d': [1, 2]}
    JsonPatchMixin.apply_delta(mock_bizobj, op, path, value=value)
    assert mock_bizobj.a == {'d': [1, 2, 3]}


def test_JsonPatchMixin_apply_delta_add_to_list_2(mock_bizobj):
    op = OP_DELTA_ADD
    value = 5
    path = [
        JsonPatchPathComponent('/', mock_bizobj),
        JsonPatchPathComponent('my_dict_list_2', mock_bizobj.my_dict_list),
        JsonPatchPathComponent('0', mock_bizobj.my_dict_list_2[0]),
        JsonPatchPathComponent('ints', mock_bizobj.my_dict_list_2[0]['ints']),
        ]

    assert mock_bizobj.my_dict_list_2 == [{'ints': [3, 4]}, {}]
    JsonPatchMixin.apply_delta(mock_bizobj, op, path, value=value)
    assert mock_bizobj.my_dict_list_2 == [{'ints': [3, 4, 5]}, {}]


def test_JsonPatchMixin_apply_delta_repl_in_list_1(mock_bizobj):
    op = OP_DELTA_REPLACE
    value = 5
    path = [
        JsonPatchPathComponent('/', mock_bizobj),
        JsonPatchPathComponent('my_dict_list_2', mock_bizobj.my_dict_list),
        JsonPatchPathComponent('0', mock_bizobj.my_dict_list_2[0]),
        JsonPatchPathComponent('ints', mock_bizobj.my_dict_list_2[0]['ints']),
        JsonPatchPathComponent('1', mock_bizobj.my_dict_list_2[0]['ints'][1]),
        ]

    assert mock_bizobj.my_dict_list_2 == [{'ints': [3, 4]}, {}]
    JsonPatchMixin.apply_delta(mock_bizobj, op, path, value=value)
    assert mock_bizobj.my_dict_list_2 == [{'ints': [3, 5]}, {}]


def test_JsonPatchMixin_apply_delta_remove_from_list_1(mock_bizobj):
    op = OP_DELTA_REMOVE
    value = None
    path = [
        JsonPatchPathComponent('/', mock_bizobj),
        JsonPatchPathComponent('a', mock_bizobj.a),
        JsonPatchPathComponent('d', mock_bizobj.a['d']),
        JsonPatchPathComponent('1', mock_bizobj.a['d'][1]),
        ]

    assert mock_bizobj.a == {'d': [1, 2]}
    JsonPatchMixin.apply_delta(mock_bizobj, op, path, value=value)
    assert mock_bizobj.a == {'d': [1]}


def test_JsonPatchMixin_apply_delta_remove_from_dict_1(mock_bizobj):
    op = OP_DELTA_REMOVE
    value = None
    path = [
        JsonPatchPathComponent('/', mock_bizobj),
        JsonPatchPathComponent('a', mock_bizobj.a),
        JsonPatchPathComponent('d', mock_bizobj.a['d']),
        ]

    assert mock_bizobj.a == {'d': [1, 2]}
    JsonPatchMixin.apply_delta(mock_bizobj, op, path, value=value)
    assert mock_bizobj.a == {'d': None}


def test_JsonPatchMixin_apply_delta_remove_from_bizobj_1(mock_bizobj):
    op = OP_DELTA_REMOVE
    value = None
    path = [
        JsonPatchPathComponent('/', mock_bizobj),
        JsonPatchPathComponent('a', mock_bizobj.a),
        ]

    assert mock_bizobj.a == {'d': [1, 2]}
    JsonPatchMixin.apply_delta(mock_bizobj, op, path, value=value)
    assert mock_bizobj.a == None


def test_JsonPatchMixin_apply_delta_invalid_1(mock_bizobj):
    op = OP_DELTA_ADD
    value = 'foo'
    path = [
        JsonPatchPathComponent('/', mock_bizobj),
        JsonPatchPathComponent('b', mock_bizobj.b),
        JsonPatchPathComponent('c', mock_bizobj.b.c),
        ]

    with pytest.raises(JsonPatchError):
        JsonPatchMixin.apply_delta(mock_bizobj, op, path, value=value)


def test_JsonPatchMixin_apply_delta_invalid_2(mock_bizobj):
    op = OP_DELTA_REMOVE
    value = None
    path = [
        JsonPatchPathComponent('/', mock_bizobj),
        JsonPatchPathComponent('a', mock_bizobj.a),
        JsonPatchPathComponent('y', None),
        ]

    with pytest.raises(JsonPatchError):
        JsonPatchMixin.apply_delta(mock_bizobj, op, path, value=value)
