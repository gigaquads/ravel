import os
import shutil

import pytest

from appyratus.test import mark

from pybiz.biz import BizObject
from pybiz.schema import fields
from pybiz.dao.file_system_dao import FileSystemDao


@pytest.fixture(scope='module')
def Thing():
    class Thing(BizObject):
        color = fields.String()

    return Thing


@pytest.fixture(scope='function')
def dao(Thing):
    shutil.rmtree('/tmp/fs_dao_test', ignore_errors=True)

    dao = FileSystemDao(root='/tmp/fs_dao_test', ftype='yaml')
    dao.bind(Thing)

    return dao


@mark.integration
def test__file_creates_as_expected(dao):
    record = dao.create({'a': 1, 'b': 2})
    assert os.path.exists(
        os.path.join(dao.paths.data, record['_id'] + '.yml')
    )


@mark.integration
def test__fetches_expected_file(dao):
    record = dao.create({'a': 1, 'b': 2})
    fetched_record = dao.fetch(record['_id'])
    assert record == fetched_record
