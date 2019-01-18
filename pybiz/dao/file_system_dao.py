import os
import uuid

from typing import Text, List, Set, Dict, Tuple
from datetime import datetime

from appyratus.utils import (
    DictAccessor,
    DictUtils,
    StringUtils,
)

from appyratus.enum import EnumValueStr
from appyratus.files import File, Yaml, Json

from .base import Dao
from .dict_dao import DictDao


class FileType(EnumValueStr):

    @staticmethod
    def values():
        return {'json', 'yaml'}


class FileSystemDao(Dao):

    FILE_TYPE_NAME_2_CLASS = {
        FileType.json: Json,
        FileType.yaml: Yaml,
    }

    def __init__(
        self,
        root: Text,
        ftype: FileType = None,
        extensions: Set[Text] = None,
    ):
        # convert the ftype string arg into a File class ref
        self.ftype = self.FILE_TYPE_NAME_2_CLASS[ftype]

        # self.paths is where we store named file paths
        self.paths = DictAccessor({'root': root})

        # set of recognized (case-normalized) file extensions
        self.extensions = {
            ext.lower() for ext in (
                extensions or self.ftype.extensions()
            )
        }

    def mkpath(self, fname: Text) -> Text:
        fname = self.ftype.format_file_name(fname)
        return os.path.join(self.paths.data, fname)

    def bind(self, bizobj_type):
        super().bind(bizobj_type)
        self.paths.data = os.path.join(
            self.paths.root, StringUtils.snake(bizobj_type.__name__)
        )
        os.makedirs(self.paths.data, exist_ok=True)

    def next_id(self):
        return uuid.uuid4().hex

    def exists(self, fname: Text) -> bool:
        return File.exists(self.mkpath(fname))

    def create(self, record: Dict) -> Dict:
        record.setdefault('_id', self.next_id())
        return self.update(record['_id'], record)

    def create_many(self, records):
        for record in records:
            self.create(record)

    def fetch(self, _id, fields=None) -> Dict:
        fpath = self.mkpath(_id)
        record = self.ftype.from_file(fpath)
        return record

    def fetch_many(self, _ids: List, fields: List = None) -> Dict:
        if _ids:
            records = {_id: self.fetch(_id, fields) for _id in _ids}
        else:
            records = {}
            for fname in os.listdir(self.path.data):
                base, ext = os.path.splitext(fname)
                if (ext and ext[1:].lower() in self.extensions):
                    if '_id' not in record:
                        record['_id'] = os.path.basename(base)
                    records[record['_id']] = record
        return records

    def update(self, _id, data: Dict) -> Dict:
        fpath = self.mkpath(_id)
        base_record = self.ftype.from_file(fpath)
        if base_record:
            record = DictUtils.merge(base_record, data)
            self.ftype.to_file(file_path=fpath, data=merged_record)
        else:
            self.ftype.to_file(file_path=fpath, data=data)
            record = data
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
        raise NotImplementedError()
