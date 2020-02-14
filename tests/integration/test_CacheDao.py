
import pytest

from appyratus.test import mark

from pybiz.biz import Resource
from pybiz.schema import fields
from pybiz.store.simulation_store import SimulationStore
from pybiz.store.cache_store import CacheStore


@pytest.fixture(scope='module')
def Thing():
    class Thing(Resource):
        color = fields.String(default='red')

    return Thing


@pytest.fixture(scope='function')
def store(Thing):
    store = CacheStore(persistence=SimulationStore(), cache=SimulationStore())
    store.bind(Thing)
    return store


@mark.integration
def test__create_upserts_to_cache(store, Thing):
    thing = Thing()
    record = store.create(thing.data)
    assert store.cache.records[record['_id']] == record
    assert store.persistence.records[record['_id']] == record


@mark.integration
def test__data_is_correctly_removed_fromo_cache(store, Thing):
    thing = Thing()
    record = store.create(thing.data)
    _id = record['_id']

    store.persistence.delete(_id)

    assert store.cache.exists(_id)

    store.fetch(_id)

    assert not store.cache.exists(_id)


@mark.integration
def test__data_is_correctly_updated_in_cache(store, Thing):
    thing = Thing()
    record = store.create(thing.data)
    _id = record['_id']

    store.persistence.update(_id, {'color': 'purple'})

    assert store.cache.fetch(_id)['color'] != 'purple'

    store.fetch(_id)

    assert store.cache.fetch(_id)['color'] == 'purple'


@mark.integration
def test__data_is_correctly_inserted_in_cache(store, Thing):
    thing = Thing()
    record = store.create(thing.data)
    _id = record['_id']

    store.cache.delete(_id)

    assert not store.cache.exists(_id)

    store.fetch(_id)

    assert store.cache.exists(_id)
