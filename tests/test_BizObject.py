import os

import pytest
import mock

from pybiz.biz import BizObject
from pybiz.schema import Schema, Int, Str, Nested


@pytest.fixture(scope='module')
def ChildSchema():
    class ChildSchema(Schema):
        my_str = Str()
    return ChildSchema


@pytest.fixture(scope='module')
def ParentSchema(ChildSchema):
    class ParentSchema(Schema):
        my_child = Nested(ChildSchema())
        my_str = Str()
    return ParentSchema


@pytest.fixture(scope='module')
def Parent(ParentSchema):
    class Parent(BizObject):
        @classmethod
        def schema(cls):
            return ParentSchema
    return Parent


@pytest.fixture(scope='module')
def Child(ChildSchema):
    class Child(BizObject):
        @classmethod
        def schema(cls):
            return ChildSchema
    return Child


def test_BizObject_init(Parent):
    bizobj = Parent()
    assert not bizobj.dirty

    bizobj = Parent(my_str='x')
    assert 'my_str' in bizobj.dirty
    assert 'my_child' not in bizobj.dirty


def test_BizObject_init_data(Parent, Child):
    child = Child()
    parent = Parent({'my_str': 'x'}, my_child=child)
    assert parent.data == {'my_str': 'x', 'my_child': child}

    parent = Parent({'my_str': 'x'}, my_str='y')
    assert parent.data == {'my_str': 'y'}


def test_BizObject_dirty(Parent):
    bizobj = Parent()

    bizobj.my_str = 'x'
    assert 'my_str' in bizobj.dirty

    bizobj.clear_dirty()
    assert not bizobj.dirty

    bizobj['my_str'] = 'x'
    assert 'my_str' in bizobj.dirty

    bizobj.clear_dirty()
    assert not bizobj.dirty

    setattr(bizobj, 'my_str', 'x')
    assert 'my_str' in bizobj.dirty


def test_BizObject_dirty_nested(Parent, Child):
    bizobj = Parent(my_child=Child())
    bizobj.my_child.my_str = 'x'
    assert 'my_str' in bizobj.my_child.dirty
    assert 'my_child' in bizobj.dirty


def test_BizObject_dao_provider(Parent):
    old_environ = os.environ.copy()
    os.environ.clear()

    bizobj = Parent()
    os.environ['DAO_PROVIDER'] = 'x'
    assert bizobj.dao_provider() == os.environ['DAO_PROVIDER']

    os.environ['PARENT_DAO_PROVIDER'] = 'y'
    assert bizobj.dao_provider() == os.environ['PARENT_DAO_PROVIDER']

    os.environ.clear()
    os.environ.update(old_environ)


def test_BizObject_setitem(Child):
    bizobj = Child()
    bizobj['my_str'] = 'x'
    assert bizobj['my_str'] == 'x'
    with pytest.raises(KeyError):
        bizobj['foo'] = 'x'


def test_BizObject_save(Parent, Child):
    bizobj = Parent(my_child=Child())
    new_id = 1

    mock_dao = mock.MagicMock()
    mock_dao.save.return_value = new_id
    mock_dao.fetch.return_value = {}

    bizobj._dao_manager = mock.MagicMock()
    bizobj._dao_manager.get_dao_for_bizobj.return_value = mock_dao

    bizobj.my_str = 'x'
    assert 'my_str' in bizobj.dirty

    bizobj.save()
    bizobj.dao.save.assert_called_once_with(
            {'my_str': 'x', 'my_child': {}}, _id=None)

    assert bizobj._id == new_id
    assert bizobj.my_str == 'x'
    assert not bizobj.dirty


def test_BizObject_save_and_fetch(Parent, Child):
    bizobj = Parent(my_child=Child())
    new_id = 1
    new_my_str = 'x_saved'

    mock_dao = mock.MagicMock()
    mock_dao.save.return_value = new_id
    mock_dao.fetch.return_value = {'my_str': new_my_str}

    bizobj._dao_manager = mock.MagicMock()
    bizobj._dao_manager.get_dao_for_bizobj.return_value = mock_dao

    bizobj.my_str = 'x'
    assert 'my_str' in bizobj.dirty

    bizobj.save(fetch=True)
    bizobj.dao.save.called_once_with({'my_str': 'x'}, _id=None)
    assert bizobj._id == new_id
    assert bizobj.my_str == new_my_str
    assert not bizobj.dirty


def test_BizObject_save_nested(Parent, Child):
    new_id = 1
    mock_dao = mock.MagicMock()
    mock_dao.save.return_value = new_id
    mock_dao.fetch.return_value = {}

    BizObject._dao_manager = mock.MagicMock()
    BizObject._dao_manager.get_dao_for_bizobj.return_value = mock_dao

    bizobj = Parent(my_child=Child())
    bizobj.my_child.my_str = 'x'
    bizobj.my_child.save()

    bizobj.my_child.dao.save.assert_called_once_with(
            {'my_str': 'x'}, _id=None)


def test_BizObject_save_nested_through_parent(Parent, Child):
    new_id = 1
    def mock_dao():
        mock_dao = mock.MagicMock()
        mock_dao.save.return_value = new_id
        mock_dao.fetch.return_value = {}
        return mock_dao

    BizObject._dao_manager = mock.MagicMock()
    BizObject._dao_manager.get_dao_for_bizobj.return_value = mock_dao()

    bizobj = Parent(my_child=Child())
    bizobj.my_child.my_str = 'x'
    bizobj.save()

    assert not bizobj.my_child.dirty
    assert not bizobj.dirty

    bizobj.my_child.dao.save.assert_any_call({'my_str': 'x'}, _id=None)

    bizobj.dao.save.assert_any_call(
            {'my_child': {'my_str': 'x', '_id': new_id}}, _id=None)

