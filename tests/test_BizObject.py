import pytest

from pybiz.biz import BizObject
from pybiz.schema import Schema, Int, Str, Nested


class ThingSchema(Schema):
    label = Str()


class UserSchema(Schema):
    _id = Int()
    public_id = Str()
    name = Str()
    age = Int()
    thing = Nested(ThingSchema())


class User(BizObject):
    @classmethod
    def schema(cls):
        return UserSchema


class Thing(BizObject):
    @classmethod
    def schema(cls):
        return ThingSchema


@pytest.fixture(scope='function')
def thing():
    thing = Thing(
            _id=2,
            public_id='2'*32,
            label='foobar')
    thing.clear_dirty()
    return thing


@pytest.fixture(scope='function')
def user(thing):
    user = User(
            _id=1,
            public_id='1'*32,
            name='Samantha',
            age=35,
            thing=thing)
    user.clear_dirty()
    return user


def test_dirty(user):
    user.age = 5
    assert 'age' in user.dirty


def test_dirty_nested(user, thing):
    user.thing.label = 'thing'
    assert 'thing' in user.dirty
    assert 'label' in user.thing.dirty
