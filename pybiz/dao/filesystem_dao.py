import os
import uuid
import glob

from typing import Text, List, Set, Dict, Tuple
from collections import defaultdict
from datetime import datetime

from appyratus.env import Environment
from appyratus.files import BaseFile, Yaml
from appyratus.utils import (
    DictObject,
    DictUtils,
    StringUtils,
)

from pybiz.util.misc_functions import import_object

from .base import Dao
from .python_dao import PythonDao


class FilesystemDao(Dao):

    env = Environment()
    root = None
    paths = None

    def __init__(
        self,
        ftype: Text = None,
        extension: Text = None,
    ):
        super().__init__()

        self._paths = DictObject()
        self._cache_dao = PythonDao()

        # convert the ftype string arg into a File class ref
        if not ftype:
            self._ftype = Yaml
        elif not isinstance(ftype, BaseFile):
            self._ftype = import_object(ftype)

        assert issubclass(self.ftype, BaseFile)

        if extension is None:
            self._extension = sorted(list(self.ftype.extensions()))[0].lower()
        else:
            self._extension = extension.lower()

    @property
    def paths(self):
        return self._paths

    @property
    def extension(self):
        return self._extension

    @property
    def ftype(self):
        return self._ftype

    @classmethod
    def on_bootstrap(cls, ftype: Text = None, root: Text = None):
        cls.ftype = import_object(ftype) if ftype else Yaml
        cls.root = root or cls.root
        assert cls.root

    def on_bind(self, biz_class, root: Text = None, ftype: BaseFile = None):
        """
        Ensure the data dir exists for this BizObject type.
        """
        if isinstance(ftype, str):
            self.ftype = import_object(ftype)

        self.paths.root = root or self.root
        self.paths.records = os.path.join(
            self.paths.root, StringUtils.snake(biz_class.__name__)
        )
        os.makedirs(self.paths.records, exist_ok=True)

        self._cache_dao.bootstrap(biz_class.app)
        self._cache_dao.bind(biz_class)
        self._cache_dao.create_many(
            self.fetch_all(ignore_cache=True).values()
        )

    def create_id(self, record):
        return record.get('_id', uuid.uuid4().hex)

    def exists(self, fname: Text) -> bool:
        return BaseFile.exists(self.mkpath(fname))

    def create(self, record: Dict) -> Dict:
        _id = self.create_id(record)
        record = self.update(_id, record)
        record['_id'] = _id
        self._cache_dao.create(record)
        return record

    def create_many(self, records):
        created_records = []
        for record in records:
            created_records.append(self.create(record))
        self._cache_dao.create_many(created_records)

    def count(self) -> int:
        fnames = glob.glob(f'{self.paths.records}/*.{self._extension}')
        return len(fnames)

    def fetch(self, _id, fields=None) -> Dict:
        records = self.fetch_many([_id], fields=fields)
        return records.get(_id) if records else {}

    def fetch_many(self, _ids: List, fields: List = None, ignore_cache=False) -> Dict:
        if not _ids:
            _ids = self._fetch_all_ids()

        if not ignore_cache:
            cached_records = self._cache_dao.fetch_many(_ids)
            if cached_records:
                _ids -= cached_records.keys()
        else:
            cached_records = {}

        fields = fields if isinstance(fields, set) else set(fields or [])
        if not fields:
            fields = set(self.biz_class.schema.fields.keys())
        fields |= {'_id', '_rev'}

        records = {}

        for _id in _ids:
            fpath = self.mkpath(_id)
            record = self.ftype.read(fpath)
            if record:
                record.setdefault('_id', _id)
                record['_rev'] = record.setdefault('_rev', 0)
                records[_id] = {k: record.get(k) for k in fields}
            else:
                records['_id'] = None

        self._cache_dao.create_many(records.values())
        records.update(cached_records)
        return records

    def fetch_all(self, fields: Set[Text] = None, ignore_cache=False) -> Dict:
        return self.fetch_many(None, fields=fields, ignore_cache=ignore_cache)

    def update(self, _id, data: Dict) -> Dict:
        if not self.exists(_id):
            # this is here, because update in this dao is can be used like #
            # upsert, but in the BizObject class, insert_defaults is only called
            # on create, not update.
            self.biz_class.insert_defaults(data)

        fpath = self.mkpath(_id)
        base_record = self.ftype.read(fpath)
        if base_record:
            # this is an upsert
            record = DictUtils.merge(base_record, data)
        else:
            record = data

        if '_id' not in record:
            record['_id'] = _id

        if '_rev' not in record:
            record['_rev'] = 0
        else:
            record['_rev'] += 1

        self._cache_dao.update(_id, record)
        self.ftype.write(path=fpath, data=record)
        return record

    def update_many(self, _ids: List, updates: List = None) -> Dict:
        return {
            _id: self.update(_id, data)
            for _id, data in zip(_ids, update)
        }

    def delete(self, _id) -> None:
        self._cache_dao.delete(_id)
        fpath = self.mkpath(_id)
        os.unlink(fpath)

    def delete_many(self, _ids: List) -> None:
        for _id in _ids:
            self.delete(_id)

    def delete_all(self):
        _ids = self._fetch_all_ids()
        self.delete_many(_ids)

    def query(self, *args, **kwargs):
        return self._cache_dao.query(*args, **kwargs)

    def mkpath(self, fname: Text) -> Text:
        fname = self.ftype.format_file_name(fname)
        return os.path.join(self.paths.records, fname)

    def _fetch_all_ids(self):
        _ids = set()
        for fpath in glob.glob(f'{self.paths.records}/*.{self.extension}'):
            fname = fpath.split('/')[-1]
            basename = os.path.splitext(fname)[0]
            _ids.add(basename)
        return _ids
