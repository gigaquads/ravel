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
    IS_RESOURCE,
    ID,
    REV,
)



class TestResourceDataStructures:
    def test_resource_builds(self, app, Tree):
        """
        Ensure that a basic Resource class is initialized by its metaclass
        correctly, following app bootstrapping.
        """
        # check basic class attributes are as expected
        assert hasattr(Tree, 'ravel')
        assert hasattr(Tree, 'Schema')
        assert hasattr(Tree, IS_RESOURCE)
        assert isinstance(Tree.Schema, type)
        assert issubclass(Tree.Schema, ravel.Schema)
        assert isinstance(Tree.Batch, type)
        assert issubclass(Tree.Batch, Batch)
        assert Tree.ravel.app is app
        assert Tree.ravel.is_abstract is False
        assert Tree.ravel.is_bootstrapped is True
        assert Tree.ravel.is_bound is True
        assert isinstance(Tree.ravel.store, ravel.Store)
        assert isinstance(Tree.ravel.schema, Tree.Schema)
        assert isinstance(Tree.ravel.resolvers, ResolverManager)
        assert Tree.ravel.defaults
        assert ID in Tree.ravel.defaults

        # ensure the expected fields, resolvers, and resolver properties exist
        expected_field_names = {REV, ID, 'name'}

        for name in expected_field_names:
            assert name in Tree.Schema.fields
            assert name in Tree.ravel.resolvers

            attr = getattr(Tree, name)
            assert isinstance(attr, ResolverProperty)
            assert isinstance(attr.resolver, Resolver)
            assert attr.resolver.name == name
            assert attr.resolver.field.name == name

        # make sure id field replacement works, replacing `Id` fields in the Schema
        # with a new Field with the same type as the referenced resource class's ID
        # field.
        tree_id_field = Tree.Schema.fields[ID]
        for k in Tree.ravel.foreign_keys:
            fk_field = Tree.Schema.fields[k]
            assert not isinstance(fk_field, ravel.Id)
            assert isinstance(fk_field, type(tree_id_field))

    def test_custom_resolver(self, Tree):
        parent = Tree(name='parent').create()
        child = Tree(name='child', parent_id=parent._id).create()
        assert child.parent
        assert child.parent._id == parent._id

    def test_generate(self, Tree):
        tree = Tree.generate()
        for k, resolver in Tree.ravel.resolvers.fields.items():
            if k == REV:
                assert k not in tree.internal.state
            else:
                assert k in tree.internal.state
                if not resolver.nullable:
                    assert tree.internal.state[k] is not None

    def test_generate_resolvers(self, Tree):
        tree = Tree.generate({'parent', 'children'})
        parent = tree.internal.state.get('parent')
        children = tree.internal.state.get('children')
        assert isinstance(parent, Tree)
        assert isinstance(children, Tree.Batch)
        assert children
