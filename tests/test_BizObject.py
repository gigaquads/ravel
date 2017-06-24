import os

import pytest
import mock

from pybiz.biz import BizObject, Relationship
from pybiz.schema import Schema, Int, Str, Object


@pytest.fixture(scope='module')
def Child():

    class Child(BizObject):

        @classmethod
        def __dao__(cls):
            dao = mock.MagicMock()
            dao.save.return_value = 1
            dao.create.return_value = 1
            dao.fetch.return_value = {}
            return dao

        my_str = Str()

    return Child


@pytest.fixture(scope='module')
def SuperParent(Child):

    class SuperParent(BizObject):

        @classmethod
        def __dao__(cls):
            dao = mock.MagicMock()
            dao.save.return_value = 1
            dao.create.return_value = 1
            dao.fetch.return_value = {}
            return dao

        my_str_super = Str()
        my_child_super = Relationship(Child)

    return SuperParent


@pytest.fixture(scope='module')
def Parent(SuperParent, Child):

    class Parent(SuperParent):

        @classmethod
        def __dao__(cls):
            dao = mock.MagicMock()
            dao.save.return_value = 1
            dao.create.return_value = 1
            dao.fetch.return_value = {}
            return dao

        my_str = Str()
        my_child = Relationship(Child)

    return Parent


def test_BizObject_inherits_relationships(Parent, SuperParent):
    """
    Make sure the derived classes inherit the fields of their super classes.
    """
    property_type = property().__class__
    for k in ['my_child', 'my_child_super']:
        assert hasattr(Parent, k)
        assert isinstance(getattr(Parent, k), property_type)
        assert k in Parent._relationships


def test_BizObject_relationship_names(Parent, SuperParent):
    """
    Make sure that the Relationship instances are aware of what they are called
    from the point of view of the BizObject class.
    """
    assert Parent._relationships['my_child'].name == 'my_child'
    assert Parent._relationships['my_child_super'].name == 'my_child_super'


def test_BizObject_init(Parent):
    bizobj = Parent()
    assert not bizobj.dirty

    bizobj = Parent(my_str='x')
    assert 'my_str' in bizobj.dirty
    assert 'my_child' not in bizobj.dirty


def test_BizObject_init_data(Parent, Child):
    child = Child(my_str='z')
    parent = Parent({'my_str': 'x'}, my_child=child)
    assert parent.data == {'my_str': 'x'}
    assert parent.relationships == {'my_child': child, 'my_child_super': None}

    # test that kwarg data overrides dict data passed to ctor
    parent = Parent({'my_str': 'x'}, my_str='y')
    assert parent.data == {'my_str': 'y'}


def test_BizObject_dump(Parent, Child):
    child = Child(my_str='z')
    parent = Parent({'my_str': 'x'}, my_child=child)

    dumped_data = parent.dump()
    assert dumped_data == {
        'my_str': 'x',
        'my_child': {'my_str': 'z'},
        'my_child_super': None,
        }


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
    #assert 'my_child' in bizobj.dirty


def test_BizObject_setitem(Child):
    bizobj = Child()
    bizobj['my_str'] = 'x'
    assert bizobj['my_str'] == 'x'
    with pytest.raises(KeyError):
        bizobj['foo'] = 'x'


def test_BizObject_save(Parent, Child):
    bizobj = Parent(my_child=Child(my_str='z'))
    new_id = 1

    Parent._dao_manager = mock.MagicMock()
    Parent._dao_manager.get_dao.return_value = Parent.__dao__()

    Child._dao_manager = mock.MagicMock()
    Child._dao_manager.get_dao.return_value = Child.__dao__()

    bizobj.my_str = 'x'
    assert 'my_str' in bizobj.dirty

    bizobj.save()

    bizobj.dao.create.assert_called_once_with(
        {'my_str': 'x', 'my_child': {'my_str': 'z', '_id': 1}})

    bizobj.my_child.dao.create.assert_called_once_with({'my_str': 'z'})

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
    bizobj._dao_manager.get_dao.return_value = mock_dao

    bizobj.my_str = new_my_str
    assert 'my_str' in bizobj.dirty

    bizobj.save(fetch=True)
    bizobj.dao.save.called_once_with({'my_str': new_my_str}, _id=None)
    assert bizobj._id == new_id
    assert bizobj.my_str == new_my_str
    assert not bizobj.dirty


def test_BizObject_save_nested(Parent, Child):
    new_id = 1
    mock_dao = mock.MagicMock()
    mock_dao.save.return_value = new_id
    mock_dao.fetch.return_value = {}

    BizObject._dao_manager = mock.MagicMock()
    BizObject._dao_manager.get_dao.return_value = mock_dao

    bizobj = Parent(my_child=Child())
    bizobj.my_child.my_str = 'x'
    bizobj.my_child.save()

    bizobj.my_child.dao.create.assert_any_call({'my_str': 'x'})


def test_BizObject_save_nested_through_parent(Parent, Child):
    new_id = 1
    def mock_dao():
        mock_dao = mock.MagicMock()
        mock_dao.save.return_value = new_id
        mock_dao.fetch.return_value = {}
        return mock_dao

    BizObject._dao_manager = mock.MagicMock()
    BizObject._dao_manager.get_dao.return_value = mock_dao()

    bizobj = Parent(my_child=Child())
    bizobj.my_child.my_str = 'x'
    bizobj.save()

    assert not bizobj.my_child.dirty
    assert not bizobj.dirty

    bizobj.my_child.dao.create.assert_any_call({'my_str': 'x'})

    bizobj.dao.create.assert_any_call(
        {'my_child': {'my_str': 'x', '_id': new_id}})

