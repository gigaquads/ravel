
import pytest

from appyratus.test import mark

from pybiz.biz import BizObject
from pybiz.schema import fields
from pybiz.dao.python_dao import PythonDao
from pybiz.dao.cache_dao import CacheDao


@pytest.fixture(scope='module')
def Thing():
    class Thing(BizObject):
        color = fields.String(default='red')

    return Thing


@pytest.fixture(scope='function')
def dao(Thing):
    dao = CacheDao(persistence=PythonDao(), cache=PythonDao())
    dao.bind(Thing)
    return dao


@mark.integration
def test__create_upserts_to_cache(dao, Thing):
    thing = Thing()
    record = dao.create(thing.data)
    assert dao.cache.records[record['_id']] == record
    assert dao.persistence.records[record['_id']] == record


@mark.integration
def test__data_is_correctly_removed_fromo_cache(dao, Thing):
    thing = Thing()
    record = dao.create(thing.data)
    _id = record['_id']

    dao.persistence.delete(_id)

    assert dao.cache.exists(_id)

    dao.fetch(_id)

    assert not dao.cache.exists(_id)


@mark.integration
def test__data_is_correctly_updated_in_cache(dao, Thing):
    thing = Thing()
    record = dao.create(thing.data)
    _id = record['_id']

    dao.persistence.update(_id, {'color': 'purple'})

    assert dao.cache.fetch(_id)['color'] != 'purple'

    dao.fetch(_id)

    assert dao.cache.fetch(_id)['color'] == 'purple'


@mark.integration
def test__data_is_correctly_inserted_in_cache(dao, Thing):
    thing = Thing()
    record = dao.create(thing.data)
    _id = record['_id']

    dao.cache.delete(_id)

    assert not dao.cache.exists(_id)

    dao.fetch(_id)

    assert dao.cache.exists(_id)
