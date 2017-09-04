import os

import pytest
import mock

from pybiz import schema as fields
from pybiz.biz import BizObject, Relationship
from pybiz.schema import Schema, Int, Str, Object


@pytest.fixture(scope='module')
def Child():

    class Child(BizObject):
        @classmethod
        def __dao__(cls):
            dao = mock.MagicMock()
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
            return dao

        my_str = Str()
        my_child = Relationship(Child)

    return Parent


@pytest.fixture(scope='module')
def BizFields():
    class BizFields(BizObject):
        floaty = fields.Float(allow_none=True)
        floaty_default = fields.Float(default=7.77, allow_none=True)

    return BizFields


def test_BizObject_inherits_relationships(Parent, SuperParent):
    """
    Make sure the derived classes inherit the fields of their super classes.
    """
    property_type = property().__class__
    for k in ['my_child', 'my_child_super']:
        assert hasattr(Parent, k)
        assert isinstance(getattr(Parent, k), property_type)
        assert k in Parent.relationships


def test_BizObject_relationship_names(Parent, SuperParent):
    """
    Make sure that the Relationship instances are aware of what they are called
    from the point of view of the BizObject class.
    """
    assert Parent.relationships['my_child'].name == 'my_child'
    assert Parent.relationships['my_child_super'].name == 'my_child_super'


def test_BizObject_init(Parent):
    bizobj = Parent()
    assert bizobj.dirty == {'public_id'}

    bizobj = Parent(my_str='x')
    assert 'my_str' in bizobj.dirty
    assert 'my_child' not in bizobj.dirty


def test_BizObject_init_data(Parent, Child):
    parent_data = {'my_str': 'x', 'public_id': '1'*32}
    child = Child(my_str='z')
    parent = Parent(parent_data, my_child=child)
    assert parent.data == parent_data
    assert parent._related_bizobjs == {'my_child': child}

    # test that kwarg data overrides dict data passed to ctor
    parent = Parent({'my_str': 'x'}, my_str='y')
    assert parent.data['my_str'] == 'y'


def test_BizObject_dump(Parent, Child):
    child = Child(my_str='z')
    parent = Parent({'my_str': 'x'}, my_child=child)

    dumped_data = parent.dump()
    assert dumped_data == {
        'id': parent.public_id,
        'my_str': 'x',
        'my_child': {
            'my_str': 'z',
            'id': child.public_id,
        },
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
    Parent._dao_manager.get_dao.return_value = parent_dao = Parent.__dao__()

    Child._dao_manager = mock.MagicMock()
    Child._dao_manager.get_dao.return_value = child_dao = Child.__dao__()

    child_dao.create.return_value = {'_id': 2, 'my_str': 'z'}
    parent_dao.create.return_value = {
        '_id': 1,
        'my_str': 'x',
        'my_child': child_dao.create.return_value,
    }

    bizobj.my_str = 'x'
    assert 'my_str' in bizobj.dirty

    bizobj.save()

    bizobj.dao.create.assert_called_once_with(
        data={
            'public_id': bizobj.public_id,
            'my_str': 'x',
            'my_child': {
                'my_str': 'z',
                'public_id': bizobj.my_child.public_id,
                '_id': 2
            }})

    bizobj.my_child.dao.create.assert_called_once_with(
         data={
            'my_str': 'z',
            'public_id': bizobj.my_child.public_id,
            })

    assert bizobj._id == new_id
    assert bizobj.my_str == 'x'
    assert not bizobj.dirty


def test_BizObject_save_and_fetch(Parent, Child):
    bizobj = Parent(my_child=Child())
    new_id = 1
    new_my_str = 'x_saved'

    Parent._dao_manager = mock.MagicMock()
    Parent._dao_manager.get_dao.return_value = parent_dao = Parent.__dao__()

    parent_dao.update.return_value = {'_id': new_id, 'my_str': new_my_str}
    parent_dao.fetch.return_value = {'_id': new_id, 'my_str': new_my_str}
    parent_dao.create.return_value = {'_id': new_id, 'my_str': new_my_str}

    bizobj.my_str = new_my_str

    assert 'my_str' in bizobj.dirty

    bizobj.save(fetch=True)
    bizobj.dao.create.assert_called_once_with(data={
        'my_str': new_my_str,
        'public_id': bizobj.public_id,
        'my_child': {
            'public_id': bizobj.my_child.public_id,
            'my_str': 'z',
            '_id': 2,
            }
        })

    assert bizobj._id == new_id
    assert bizobj.my_str == new_my_str
    assert not bizobj.dirty


def test_BizObject_save_nested(Parent, Child):
    mock_dao = mock.MagicMock()
    mock_dao.update.return_value = {'my_child': {'my_str': 'x', '_id': 2}}
    mock_dao.fetch.return_value = {}

    Parent._dao_manager = mock.MagicMock()
    Parent._dao_manager.get_dao.return_value = mock_dao

    Parent._dao_manager = mock.MagicMock()
    Parent._dao_manager.get_dao.return_value = mock_dao

    mock_child_dao = mock.MagicMock()
    mock_child_dao.update.return_value = 1
    mock_child_dao.update.return_value = {'my_str': 'x', '_id': 2}
    mock_child_dao.fetch.return_value = {}

    Child._dao_manager = mock.MagicMock()
    Child._dao_manager.get_dao.return_value = mock_child_dao

    bizobj = Parent(my_child=Child())
    bizobj.my_child.my_str = 'x'
    bizobj.my_child.save()

    bizobj.my_child.dao.create.assert_any_call(data={
        'public_id': bizobj.my_child.public_id,
        'my_str': 'x',
        })


def test_BizObject_save_nested_through_parent(Parent, Child):
    def mock_dao():
        mock_dao = mock.MagicMock()
        mock_dao.update.return_value = {
            '_id': 1,
            'my_child': {
                'my_str': 'x',
                '_id': 2
            }
        }
        mock_dao.fetch.return_value = {}
        return mock_dao

    Parent._dao_manager = mock.MagicMock()
    Parent._dao_manager.get_dao.return_value = mock_dao()

    child_mock_dao = mock.MagicMock()
    child_mock_dao.update.return_value = {'my_str': 'x', '_id': 2}
    child_mock_dao.fetch.return_value = {}

    Child._dao_manager = mock.MagicMock()
    Child._dao_manager.get_dao.return_value = child_mock_dao

    bizobj = Parent(my_child=Child())
    bizobj.my_child.my_str = 'x'

    assert bizobj.my_child.dirty
    assert bizobj.dirty == {'public_id'}

    bizobj.my_child.save()

    assert not bizobj.my_child.dirty
    assert bizobj.dirty == {'public_id'}

    bizobj.my_child.dao.create.assert_any_call(data={
        'public_id': bizobj.my_child.public_id,
        'my_str': 'x',
        })

    bizobj.dao.create.assert_not_called()


@pytest.mark.default
def test_BizObject_float_will_be_none_without_default(BizFields):
    bizobj = BizFields()

    assert bizobj.floaty == None


@pytest.mark.default
def test_BizObject_sets_default_when_field_is_not_specified(BizFields):
    bizobj = BizFields()

    assert bizobj.floaty_default == 7.77


@pytest.mark.default
def test_BizObject_sets_default_when_none_is_specified(BizFields):
    bizobj = BizFields(floaty_default=None)

    assert bizobj.floaty_default == 7.77


@pytest.mark.default
def test_BizObject_default_will_not_override_input(BizFields):
    bizobj = BizFields(floaty_default=6.66)

    assert bizobj.floaty_default == 6.66
