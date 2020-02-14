import re

import pytest

from pprint import pprint

from pybiz import Resource, fields
from pybiz.store import SimulationStore, FilesystemStore
from pybiz.schema import String

try:
    from pybiz.contrib.sqlalchemy import SqlalchemyStore
except:
    SqlalchemyStore = None

try:
    from pybiz.contrib.redis import RedisStore
except:
    RedisStore = None


DAO_TYPES = {
    'py': SimulationStore,
    'fs': FilesystemStore,
    'redis': RedisStore,
    'sa': SqlalchemyStore,
}

DAO_TYPES = dict([(k, v) for k, v in DAO_TYPES.items() if v is not None])
DAO_INSTANCES = dict([(k, v()) for k, v in DAO_TYPES.items()])


def new_biz_class(fields=None, name=None):
    name = name or 'CrashDummy'
    return type(name, (Resource, ), fields or {'name': String()})


def bootstrap_all():
    SimulationStore.bootstrap()
    FilesystemStore.bootstrap(root='/tmp/store-test')
    SqlalchemyStore.bootstrap(url='sqlite://')
    RedisStore.bootstrap(db=0)


def bind_all(fields=None):
    DAO_INSTANCES['py'].bind(new_biz_class(fields=fields))
    DAO_INSTANCES['fs'].bind(new_biz_class(fields=fields))
    if DAO_INSTANCES['sa']:
        DAO_INSTANCES['sa'].bind(new_biz_class(fields=fields))
    if DAO_INSTANCES['redis']:
        DAO_INSTANCES['redis'].bind(new_biz_class(fields=fields))


def setup_tests():
    SqlalchemyStore.create_tables()

bootstrap_all()
#bind_all()

@pytest.mark.integration
def test_create():
    bind_all({  # make this idempotent
        'name': fields.String(),
        'child': fields.Nested({'dob': fields.Int()}),
        'colors': fields.List(fields.String()),
    })

    setup_tests()  # make this idempotent
    SqlalchemyStore.connect()

    try:
        results = {}

        for store in DAO_INSTANCES.values():
            result = store.create({
                'name': 'Romulan',
                'child': {'dob': 1},
                'colors': ['red', 'green', 'blue'],
            })
            record_id = result.pop('_id')

            assert isinstance(record_id, str)
            assert re.match(r'[a-z0-9]{32}', record_id)

            results[store] = result
            items = list(results.items())

        for i in range(len(results)):
            store_i, result_i = items[i]
            for j in range(i, len(results)):
                store_j, result_j = items[j]
                if not result_i == result_j:
                    print(store_i, store_j)
                    assert result_i == result_j
    finally:
        SqlalchemyStore.close()
