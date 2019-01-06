import os

from typing import Text, Type, List, Set, Dict, Tuple
from copy import deepcopy
from datetime import datetime

from appyratus.utils import (
    DictAccessor,
    DictUtils,
    StringUtils,
    TimeUtils,
    StringUtils,
)

from appyratus.files import File, Yaml, Json

from .base import Dao
from .dict_dao import DictDao

# TODO: move extensiosn into base File
#       class as required staticmethod

class FileSystemDao(Dao):
    def __init__(
        self,
        root: Text,
        ftype: Type[File] = None,
        extensions: Set[Text] = None,
        prefetch=True,
    ):
        self.paths = DictAccessor({'root': root})
        self.ftype = ftype or Yaml
        self.extensions = extensions or {'yml', 'yaml'}
        self.prefetch = prefetch
        self.cache = None

    def bind(self, bizobj_type):
        super().bind(bizobj_type)
        self.paths.data = os.path.join(
            self.paths.root, StringUtils.snake(bizobj_type.__name__)
        )
        self.cache = Cache(bizobj_type)
        if self.prefetch:
            # recursively load all recognized files in
            # root data dir into cache..
            for root, dirnames, fnames in os.walk(self.paths.data):
                for fname in fnames:
                    base, ext = os.path.splitext(fname)
                    if not (ext and ext[1:] in self.extensions):
                        continue
                    fpath = os.path.join(root, fname)
                    with open(fpath) as fin:
                        record = self.ftype.from_file(fpath)
                        if '_id' not in record:
                            record['_id'] = os.path.basename(base)
                        self.cache.insert(record)

    def build_fpath(self, file_name: Text) -> Text:
        return os.path.join(
            self.paths.data,
            self.ftype.format_file_name(file_name)
        )

    def exists(self, file_name: Text) -> bool:
        return File.exists(self.build_fpath(file_name))

    def create(self, record: Dict) -> Dict:
        record['_id'] = record['name']
        fpath = self.build_fpath(record['name'])

        self.ftype.to_file(file_path=fpath, data=record)
        self.cache.insert(record)

        return deepcopy(record)

    def update(self, _id, data: Dict) -> Dict:
        fpath = self.build_fpath(_id)
        base_record = self.ftype.from_file(fpath)
        merged_record = DictUtils.merge(base_record, data)

        self.cache.insert(merged_record)

        return deepcopy(merged_record)

    def fetch(self, _id, fields=None) -> Dict:
        fpath = self.build_fpath(_id)
        cached_record, modified_at = self.cache.get(_id)

        if cached_record is not None:
            stat = os.stat(fpath)
            if stat and (modified_at >= stat.st_mtime):
                return deepcopy(cached_record)

        record = self.ftype.from_file(fpath)
        self.cache.insert(record)

        return deepcopy(record)

    def fetch_many(self, _ids: List, fields: List = None) -> Dict:
        return {_id: self.fetch(_id, fields) for _id in _ids}

    def delete(self, _id) -> None:
        os.unlink(_id)
        self.cache.invalidate(_id)

    def update_many(self, _ids: List, updates: List = None) -> Dict:
        return {
            _id: self.update(_id, data)
            for _id, data in zip(_ids, update)
        }

    def delete_many(self, _ids: List) -> Dict:
        for _id in _ids:
            self.delete(_id)
        return {}

    def query(self, predicate: 'Predicate', **kwargs):
        raise NotImplementedError()

    def create_many(self, records):
        raise NotImplementedError()


class Cache(object):
    def __init__(self, bizobj_type):
        self.dao = DictDao(type_name=bizobj_type.__name__)
        self.modified_at = {}

    def insert(self, record) -> None:
        cache_record = deepcopy(record)
        self.modified_at[cache_record['_id']] = TimeUtils.utc_timestamp()
        self.dao.update(cache_record['_id'], cache_record)

    def get(self, _id) -> Tuple[Dict, int]:
        return (self.dao.fetch(_id), self.modified_at.get(_id))

    def invalidate(self, _id):
        self.modified_at.pop(_id, None)
        self.dao.delete(_id)


if __name__ == '__main__':
    from pybiz.biz import BizObject
    from pybiz.schema import fields
    from pybiz.api.repl import ReplRegistry

    class User(BizObject):
        name = fields.String()
        age = fields.Int()

    dao = FileSystemDao(root='/Users/dgabriele/Tmp/data')
    dao.bind(User)

    repl = ReplRegistry()
    repl.manifest.process()
    repl.start({
        'User': User,
        'dao': dao,
    })
