import ravel
import pytest

from appyratus.utils import DictObject

from ravel import String, Int, Float, Bool
from ravel.batch import Batch, BatchResolverProperty

@pytest.fixture(scope='function')
def trees(Tree):
    return Tree.Batch.generate(count=5).create()


def test_factory_method(Tree, trees):
    assert isinstance(getattr(Tree.Batch, 'ravel', None), DictObject)
    assert Tree.Batch.ravel.owner is Tree
    assert Tree.Batch.ravel.indexed_field_types \
            == Tree.Batch.get_indexed_field_types()
    assert Tree.Batch.ravel.properties
    assert len(Tree.Batch.ravel.properties) == len({
        resolver for resolver in Tree.ravel.resolvers.fields.values()
        if isinstance(resolver.field, Tree.Batch.get_indexed_field_types())
    })

    # ensure that BatchResolverProperty are created for each indexable
    # field type on its associated Resource class.
    for resolver in Tree.ravel.resolvers.fields.values():
        if isinstance(resolver.field, Batch.get_indexed_field_types()):
            attr = getattr(Tree.Batch, resolver.name, None)
            assert isinstance(attr, BatchResolverProperty)
            print(
                f'Batch class has expected resolver property: {resolver.name}'
            )

    trees = Tree.Batch()

    for resolver in Tree.ravel.resolvers.fields.values():
        if isinstance(resolver.field, Batch.get_indexed_field_types()):
            assert resolver.name in trees.internal.indexes
            print(f'Batch object has expected index: {resolver.name}')
        else:
            assert resolver.name not in trees.internal.indexes


def test_insert(basic_resource, trees):
    assert not len(trees)

    trees.insert(0, basic_resource)

    assert len(trees) == 1
    assert trees.internal.resources[0] is basic_resource
    for k in trees.ravel.properties:
        v = basic_resource[k]
        assert trees.internal.indexes[k][v] == {basic_resource}

def test_insert(Tree, trees): pass
def test_append(Tree, trees): pass
def test_extend(Tree, trees): pass
def test_appendleft(Tree, trees): pass
def test_extendleft(Tree, trees): pass
def test_pop(Tree, trees): pass
def test_popleft(Tree, trees): pass

def test_where(Tree, trees):
    names = ['A', 'B', 'C']
    trees.extend([
        Tree(name=name).create()
        for name in names
    ])

    for name in names:
        predicate = Tree.name == name
        filtered_batch = trees.where(predicate)
        assert filtered_batch.internal.indexed == False
        assert len(filtered_batch) == 1
        filtered_resource = filtered_batch.internal.resources[0]
        assert filtered_resource.name == name

        # do the same thing but make sure "indexed" is set.
        filtered_batch = trees.where(predicate, indexed=True)
        assert filtered_batch.internal.indexed == True
