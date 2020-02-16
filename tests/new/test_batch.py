import pybiz
import pytest

from appyratus.utils import DictObject

from pybiz import String, Int, Float, Bool
from pybiz.biz.batch import Batch, BatchResolverProperty



def test_factory_method(BasicResource, BasicBatch, basic_batch):
    assert isinstance(getattr(BasicBatch, 'pybiz', None), DictObject)
    assert BasicBatch.pybiz.owner is BasicResource
    assert BasicBatch.pybiz.indexed_field_types \
            == BasicBatch.get_indexed_field_types()
    assert BasicBatch.pybiz.resolver_properties
    assert len(BasicBatch.pybiz.resolver_properties) == len({
        resolver for resolver in BasicResource.pybiz.resolvers.fields.values()
        if isinstance(resolver.field, BasicBatch.get_indexed_field_types())
    })

    # ensure that BatchResolverProperty are created for each indexable
    # field type on its associated Resource class.
    for resolver in BasicResource.pybiz.resolvers.fields.values():
        if isinstance(resolver.field, Batch.get_indexed_field_types()):
            attr = getattr(BasicBatch, resolver.name, None)
            assert isinstance(attr, BatchResolverProperty)
            print(
                f'Batch class has expected resolver property: {resolver.name}'
            )

    basic_batch = BasicBatch()

    for resolver in BasicResource.pybiz.resolvers.fields.values():
        if isinstance(resolver.field, Batch.get_indexed_field_types()):
            assert resolver.name in basic_batch.internal.indexes
            print(f'Batch object has expected index: {resolver.name}')
        else:
            assert resolver.name not in basic_batch.internal.indexes


def test_insert(basic_resource, basic_batch):
    assert not len(basic_batch)

    basic_batch.insert(0, basic_resource)

    assert len(basic_batch) == 1
    assert basic_batch.internal.resources[0] is basic_resource
    for k in basic_batch.pybiz.resolver_properties:
        v = basic_resource[k]
        assert basic_batch.internal.indexes[k][v] == {basic_resource}

def test_insert(BasicResource, basic_batch): pass
def test_append(BasicResource, basic_batch): pass
def test_extend(BasicResource, basic_batch): pass
def test_appendleft(BasicResource, basic_batch): pass
def test_extendleft(BasicResource, basic_batch): pass
def test_pop(BasicResource, basic_batch): pass
def test_popleft(BasicResource, basic_batch): pass

def test_where(BasicResource, basic_batch):
    names = ['A', 'B', 'C']
    basic_batch.extend([
        BasicResource(required_str_field=name).create()
        for name in names
    ])

    for name in names:
        predicate = BasicResource.required_str_field == name
        filtered_batch = basic_batch.where(predicate)
        assert filtered_batch.internal.indexed == False
        assert len(filtered_batch) == 1
        filtered_resource = filtered_batch.internal.resources[0]
        assert filtered_resource.required_str_field == name

        # do the same thing but make sure "indexed" is set.
        filtered_batch = basic_batch.where(predicate, indexed=True)
        assert filtered_batch.internal.indexed == True
