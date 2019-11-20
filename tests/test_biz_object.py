import os

os.environ['PYBIZ_CONSOLE_LOG_LEVEL'] = 'WARN'

from mock import MagicMock

import pytest

import pybiz

from pybiz import Application
from pybiz.constants import ID_FIELD_NAME
from pybiz.biz2.biz_object import BizObject
from pybiz.biz2.resolver import (
    Resolver,
    ResolverProperty,
    resolver,
)



@pytest.fixture(scope='function')
def app():
    return Application().bootstrap()


@pytest.fixture(scope='function')
def Dog(app):
    class Dog(BizObject):
        mother_id = pybiz.Id()
        color = pybiz.String()
        name = pybiz.String()
        age = pybiz.Int()

        @resolver
        def mother(self, resolver):
            return Dog(color='brown', age=12)

        @mother.on_get
        def mother(self, resolver, mother):
            print(f'Getting mother of {self}')

    app.bind(Dog)
    return Dog


@pytest.fixture(scope='function')
def DogMan(app, Dog):
    class DogMan(Dog):
        name = pybiz.String()

    DogMan.bootstrap(app)
    return DogMan


@pytest.fixture(scope='function')
def lassie(Dog):
    return Dog(color='brownish', age=8).save()


def test_basic_schema_creation(Dog):
    assert Dog.Schema is not None
    assert issubclass(Dog.Schema, pybiz.Schema)
    assert isinstance(Dog.pybiz.schema, Dog.Schema)

    assert isinstance(Dog.Schema.fields['color'], pybiz.String)
    assert isinstance(Dog.Schema.fields['age'], pybiz.Int)


def test_schema_creation_with_inheritance(Dog, DogMan):
    assert DogMan.Schema.fields['color'] is not Dog.Schema.fields['color']
    assert DogMan.Schema.fields['age'] is not Dog.Schema.fields['age']

    assert isinstance(DogMan.Schema.fields['color'], pybiz.String)
    assert isinstance(DogMan.Schema.fields['age'], pybiz.Int)
    assert isinstance(DogMan.Schema.fields['name'], pybiz.String)


def test_id_field_value_generated_in_ctor(Dog):
    dog = Dog()
    assert dog._id is not None

    dog = Dog(_id='1' * 32)
    assert dog._id == '1' * 32


@pytest.mark.parametrize('dirty_field_names', [
    set(),
    {'_id'},
    {'name'},
    {'color'},
    {'name', 'color'},
])
def test_correct_fields_are_marked_dirty(Dog, dirty_field_names):
    dog = Dog(Dog.pybiz.schema.generate()).clean()
    dog.clean()
    assert not dog.dirty
    dog.mark(dirty_field_names)
    assert set(dog.dirty.keys()) == dirty_field_names

def test_id_fields_are_replaced(Dog):
    replacement_field = Dog.replace_id_field(pybiz.Id())
    assert not isinstance(Dog.Schema.fields[ID_FIELD_NAME], pybiz.Id)
    assert isinstance(Dog.Schema.fields[ID_FIELD_NAME], type(replacement_field))


def test_field_property_gets_value(Dog):
    dog = Dog()
    dog.internal.state['color'] = 'red'
    assert dog.color == 'red'


def test_field_property_sets_value(Dog):
    dog = Dog()

    assert dog.color is None
    assert 'color' not in dog.internal.state

    dog.color = 'red'
    assert 'color' in dog.internal.state
    assert dog.internal.state['color'] == 'red'


def test_field_property_deletes_value(Dog):
    dog = Dog()

    dog.color = 'red'
    assert 'color' in dog.internal.state
    assert dog.internal.state['color'] == 'red'

    del dog.color
    assert dog.color is None
    assert 'color' not in dog.internal.state


def test_resolver_is_registerd_via_decorator(Dog):
    assert 'mother' in Dog.pybiz.resolvers


def test_resolver_decorator_is_replaced_with_property(Dog):
    assert isinstance(Dog.mother, ResolverProperty)


def test_resolver_executes_correct_method(Dog):
    mother = Dog.pybiz.resolvers['mother'].execute(instance=MagicMock())
    assert mother.color == 'brown'
    assert mother.age == 12


def test_resolver_executes_on_get_when_got(Dog):
    Dog.pybiz.resolvers['mother'].on_get = on_get = MagicMock()
    dog = Dog()
    mother = dog.mother
    on_get.assert_called_once()


def test_create(Dog):
    dog = Dog(color='red', age=12).create()
    dog_data = dog.dao.fetch(_id=dog._id)
    assert dog_data == dog.internal.state


def test_update(Dog, lassie):
    dog = Dog.get(lassie._id)
    assert dog.color == 'brownish'

    lassie.update(color='red')

    dog = Dog.get(lassie._id)
    assert dog.color == 'red'


def test_resolvers_correctly_assembled(Dog):
    assert Dog.pybiz.resolvers['mother'] is Dog.mother.resolver
    assert Dog.pybiz.resolvers.untagged['mother'] is Dog.mother.resolver

