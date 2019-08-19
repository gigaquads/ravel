import re

import pytest

from pprint import pprint

from pybiz import BizObject, fields
from pybiz.dao import PythonDao, FilesystemDao
from pybiz.schema import String

try:
    from pybiz.contrib.sqlalchemy import SqlalchemyDao
except:
    SqlalchemyDao = None

try:
    from pybiz.contrib.redis import RedisDao
except:
    RedisDao = None


DAO_TYPES = {
    'py': PythonDao,
    'fs': FilesystemDao,
    'redis': RedisDao,
    'sa': SqlalchemyDao,
}

DAO_TYPES = dict([(k, v) for k, v in DAO_TYPES.items() if v is not None])
DAO_INSTANCES = dict([(k, v()) for k, v in DAO_TYPES.items()])


def new_biz_class(fields=None, name=None):
    name = name or 'CrashDummy'
    return type(name, (BizObject, ), fields or {'name': String()})


def bootstrap_all():
    PythonDao.bootstrap()
    FilesystemDao.bootstrap(root='/tmp/dao-test')
    SqlalchemyDao.bootstrap(url='sqlite://')
    RedisDao.bootstrap(db=0)


def bind_all(fields=None):
    DAO_INSTANCES['py'].bind(new_biz_class(fields=fields))
    DAO_INSTANCES['fs'].bind(new_biz_class(fields=fields))
    if DAO_INSTANCES['sa']:
        DAO_INSTANCES['sa'].bind(new_biz_class(fields=fields))
    if DAO_INSTANCES['redis']:
        DAO_INSTANCES['redis'].bind(new_biz_class(fields=fields))


def setup_tests():
    SqlalchemyDao.create_tables()

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
    SqlalchemyDao.connect()

    try:
        results = {}

        for dao in DAO_INSTANCES.values():
            result = dao.create({
                'name': 'Romulan',
                'child': {'dob': 1},
                'colors': ['red', 'green', 'blue'],
            })
            record_id = result.pop('_id')

            assert isinstance(record_id, str)
            assert re.match(r'[a-z0-9]{32}', record_id)

            results[dao] = result
            items = list(results.items())

        for i in range(len(results)):
            dao_i, result_i = items[i]
            for j in range(i, len(results)):
                dao_j, result_j = items[j]
                if not result_i == result_j:
                    print(dao_i, dao_j)
                    assert result_i == result_j
    finally:
        SqlalchemyDao.close()
