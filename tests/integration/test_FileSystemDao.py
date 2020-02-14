import os
import shutil

import pytest

from appyratus.test import mark

from pybiz.biz import Resource
from pybiz.schema import fields
from pybiz.store.filesystem_store import FilesystemStore


@pytest.fixture(scope='module')
def Thing():
    class Thing(Resource):
        color = fields.String()

    return Thing


@pytest.fixture(scope='function')
def store(Thing):
    shutil.rmtree('/tmp/fs_store_test', ignore_errors=True)

    store = FilesystemStore(root='/tmp/fs_store_test', ftype='yaml')
    store.bind(Thing)

    return store


@mark.integration
def test__file_creates_as_expected(store):
    record = store.create({'a': 1, 'b': 2})
    assert os.path.exists(
        os.path.join(store.paths.data, record['_id'] + '.yml')
    )


@mark.integration
def test__fetches_expected_file(store):
    record = store.create({'a': 1, 'b': 2})
    fetched_record = store.fetch(record['_id'])
    assert record == fetched_record
