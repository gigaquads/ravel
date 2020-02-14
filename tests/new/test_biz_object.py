import pytest
import pybiz

from pybiz.biz2 import (
    Resource,
    Resolver,
    ResolverManager,
    ResolverProperty,
    Relationship,
    Batch,
)
from pybiz.constants import (
    IS_BIZ_OBJECT_ANNOTATION,
    ID_FIELD_NAME,
    REV_FIELD_NAME,
)


def test_basic_resource_builds(app, BasicResource):
    """
    Ensure that a basic Resource class is initialized by its metaclass
    correctly, following app bootstrapping.
    """
    # check basic class attributes are as expected
    assert hasattr(BasicResource, 'pybiz')
    assert hasattr(BasicResource, 'Schema')
    assert hasattr(BasicResource, IS_BIZ_OBJECT_ANNOTATION)
    assert isinstance(BasicResource.Schema, type)
    assert issubclass(BasicResource.Schema, pybiz.Schema)
    assert isinstance(BasicResource.Batch, type)
    assert issubclass(BasicResource.Batch, Batch)
    assert BasicResource.pybiz.app is app
    assert BasicResource.pybiz.is_abstract is False
    assert BasicResource.pybiz.is_bootstrapped is True
    assert BasicResource.pybiz.is_bound is True
    assert isinstance(BasicResource.pybiz.store, pybiz.Store)
    assert isinstance(BasicResource.pybiz.schema, BasicResource.Schema)
    assert isinstance(BasicResource.pybiz.resolvers, ResolverManager)
    assert BasicResource.pybiz.defaults
    assert ID_FIELD_NAME in BasicResource.pybiz.defaults

    # ensure the expected fields, resolvers, and resolver properties exist
    expected_field_names = {
        REV_FIELD_NAME,
        ID_FIELD_NAME,
        'str_field',
        'int_field',
        'required_str_field',
        'nullable_int_field',
        'friend_id',
    }

    for name in expected_field_names:
        assert name in BasicResource.Schema.fields
        assert name in BasicResource.pybiz.resolvers

        attr = getattr(BasicResource, name)
        assert isinstance(attr, ResolverProperty)
        assert isinstance(attr.resolver, Resolver)
        assert attr.resolver.name == name
        assert attr.resolver.field.name == name

    # make sure id field replacement works, replacing `Id` fields in the Schema
    # with a new Field with the same type as the referenced resource class's ID
    # field.
    id_field = BasicResource.Schema.fields[ID_FIELD_NAME]
    friend_id_field = BasicResource.Schema.fields['friend_id']
    assert not isinstance(friend_id_field, pybiz.Id)
    assert isinstance(friend_id_field, type(id_field))


def test_custom_resolver(ResourceWithResolvers, BasicResource):
    res = ResourceWithResolvers().create()
    basic_res = res.my_resolver
    assert isinstance(basic_res, BasicResource)
    assert basic_res.required_str_field == 'x'

    assert res.myself is not None
    assert res._id == res._id


def test_create(BasicResource):
    res = BasicResource(required_str_field='x', int_field=1).create()
    assert isinstance(res._id, str)
    assert 'required_str_field' in res.internal.state
    assert 'int_field' in res.internal.state
    assert ID_FIELD_NAME in res.internal.state
    assert REV_FIELD_NAME in res.internal.state
    assert res.required_str_field == 'x'
    assert res.int_field == 1
    assert not res.dirty
