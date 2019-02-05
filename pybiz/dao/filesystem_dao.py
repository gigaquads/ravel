import os
import uuid
import glob

from typing import Text, List, Set, Dict, Tuple
from collections import defaultdict
from datetime import datetime

from appyratus.utils import (
    DictAccessor,
    DictUtils,
    StringUtils,
)
from appyratus.files import BaseFile

from pybiz.util import import_object

from .base import Dao
from .dict_dao import DictDao


class FilesystemDao(Dao):

    def __init__(
        self,
        root: Text,
        ftype: Text = None,
        extensions: Set[Text] = None,
    ):
        # convert the ftype string arg into a File class ref
        self.ftype = import_object(ftype)
        assert issubclass(self.ftype, BaseFile)

        # self.paths is where we store named file paths
        self.paths = DictAccessor({'root': root})

        # set of recognized (case-normalized) file extensions
        self.extensions = {
            ext.lower() for ext in (
                extensions or self.ftype.extensions()
            )
        }
        if extensions:
            self.extensions.update(extensions)

    def bind(self, biz_type):
        super().bind(biz_type)
        self.paths.data = os.path.join(
            self.paths.root, StringUtils.snake(biz_type.__name__)
        )
        os.makedirs(self.paths.data, exist_ok=True)

    def create_id(self, record):
        return record.get('_id', uuid.uuid4().hex)

    def exists(self, fname: Text) -> bool:
        return File.exists(self.mkpath(fname))

    def create(self, record: Dict) -> Dict:
        _id = self.create_id(record)
        record = self.update(_id, record)
        record['_id'] = _id
        return record

    def create_many(self, records):
        for record in records:
            self.create(record)

    def count(self) -> int:
        running_count = 0
        for ext in self.extensions:
            fnames = glob.glob(f'{self.paths.data}/*.{ext}')
            running_count += len(fnames)
        return running_count

    def fetch(self, _id, fields=None) -> Dict:
        records = self.fetch_many([_id], fields=fields)
        return records.get(_id) if records else None

    def fetch_many(self, _ids: List, fields: List = None) -> Dict:
        if not _ids:
            _ids = set()
            for ext in self.extensions:
                for fname in glob.glob(f'{self.paths.data}/*.{ext}'):
                    base = fname.split('.')[0]
                    _ids.add(os.path.basename(base))

        records = {}
        fields = fields if isinstance(fields, set) else set(fields or [])
        only_mtime = fields - {'_id'} == {'_rev'}

        if not only_mtime:
            for _id in _ids:
                fpath = self.mkpath(_id)
                record = self.ftype.from_file(fpath) or {}
                record.setdefault('_id', _id)
                record['_rev'] = int(os.path.getmtime(fpath))
                records[_id] = record
        else:
            for _id in _ids:
                fpath = self.mkpath(_id)
                records[_id] = {
                    '_id': _id,
                    '_rev': int(os.path.getmtime(fpath))
                }
        return records

    def fetch_all(self, fields: Set[Text] = None) -> Dict:
        return self.fetch_many(None, fields=fields)

    def update(self, _id, data: Dict) -> Dict:
        fpath = self.mkpath(_id)
        base_record = self.ftype.from_file(fpath)
        if base_record:
            record = DictUtils.merge(base_record, data)
            self.ftype.to_file(file_path=fpath, data=merged_record)
        else:
            self.ftype.to_file(file_path=fpath, data=data)
            record = data
        record['_rev'] = int(os.path.getmtime(fpath))
        return record

    def update_many(self, _ids: List, updates: List = None) -> Dict:
        return {
            _id: self.update(_id, data)
            for _id, data in zip(_ids, update)
        }

    def delete(self, _id) -> None:
        os.unlink(_id)

    def delete_many(self, _ids: List) -> None:
        for _id in _ids:
            self.delete(_id)

    def query(self, predicate: 'Predicate', **kwargs):
        return []  # not implemented

    def mkpath(self, fname: Text) -> Text:
        fname = self.ftype.format_file_name(fname)
        return os.path.join(self.paths.data, fname)
