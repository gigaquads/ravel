import uuid
import pytest
import ravel

from ravel import (
    Resource,
    Resolver,
    ResolverProperty,
    Batch,
    ResolverManager,
    Relationship,
)
from ravel.constants import (
    IS_RESOURCE_ATTRIBUTE,
    ID_FIELD_NAME,
    REV_FIELD_NAME,
)


def test_basic_resource_builds(app, BasicResource):
    """
    Ensure that a basic Resource class is initialized by its metaclass
    correctly, following app bootstrapping.
    """
    # check basic class attributes are as expected
    assert hasattr(BasicResource, 'ravel')
    assert hasattr(BasicResource, 'Schema')
    assert hasattr(BasicResource, IS_RESOURCE_ATTRIBUTE)
    assert isinstance(BasicResource.Schema, type)
    assert issubclass(BasicResource.Schema, ravel.Schema)
    assert isinstance(BasicResource.Batch, type)
    assert issubclass(BasicResource.Batch, Batch)
    assert BasicResource.ravel.app is app
    assert BasicResource.ravel.is_abstract is False
    assert BasicResource.ravel.is_bootstrapped is True
    assert BasicResource.ravel.is_bound is True
    assert isinstance(BasicResource.ravel.store, ravel.Store)
    assert isinstance(BasicResource.ravel.schema, BasicResource.Schema)
    assert isinstance(BasicResource.ravel.resolvers, ResolverManager)
    assert BasicResource.ravel.defaults
    assert ID_FIELD_NAME in BasicResource.ravel.defaults

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
        assert name in BasicResource.ravel.resolvers

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
    assert not isinstance(friend_id_field, ravel.Id)
    assert isinstance(friend_id_field, type(id_field))


def test_custom_resolver(ResourceWithResolvers, BasicResource):
    res = ResourceWithResolvers().create()
    basic_res = res.basic_friend
    assert isinstance(basic_res, BasicResource)
    assert basic_res.required_str_field == 'x'

    assert res.myself is not None
    assert res._id == res._id


def test_simulates_fields(BasicResource):
    res = BasicResource.generate()
    for k, resolver in BasicResource.ravel.resolvers.fields.items():
        assert k in res.internal.state
        if not resolver.nullable:
            assert res.internal.state[k] is not None


def test_simulates_other_resolvers(BasicResource, ResourceWithResolvers):
    res = ResourceWithResolvers.generate({'myself', 'basic_friend'})

    myself = res.internal.state.get('myself')
    assert isinstance(myself, ResourceWithResolvers)
    assert myself._id != res._id

    basic_friend = res.internal.state.get('basic_friend')
    assert isinstance(basic_friend, BasicResource)
