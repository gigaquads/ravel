import os
import glob

from typing import Text, List, Set, Dict, Tuple
from collections import defaultdict
from datetime import datetime

from appyratus.env import Environment
from appyratus.files import BaseFile, Yaml
from appyratus.schema.fields import UuidString
from appyratus.utils import (
    DictObject,
    DictUtils,
    StringUtils,
)

from pybiz.util.misc_functions import import_object
from pybiz.constants import ID_FIELD_NAME, REV_FIELD_NAME
from pybiz.exceptions import PybizError

from .base import Store
from .simulation_store import SimulationStore


class StoreError(PybizError):
    pass


class MissingBootstrapParameterError(StoreError):
    error_code = 'missing-bootstrap-param'
    error_message = 'Missing Bootstrap Parameter `{id}`'
    error_help = "Add the missing parameter to your manifest's bootstrap settings"


class FilesystemStore(Store):

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
        self._cache_store = SimulationStore()

        # convert the ftype string arg into a File class ref
        if not ftype:
            self._ftype = Yaml
        elif not isinstance(ftype, BaseFile):
            self._ftype = import_object(ftype)

        assert issubclass(self.ftype, BaseFile)

        known_extension = self._ftype.has_extension(extension)
        if known_extension:
            self._extension = known_extension
        if extension is None:
            self._extension = self._ftype.default_extension()

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
        if not cls.root:
            raise MissingBootstrapParameterError(data={'id': 'root'})

    def on_bind(self, biz_class, root: Text = None, ftype: BaseFile = None):
        """
        Ensure the data dir exists for this Resource type.
        """
        if isinstance(ftype, str):
            self.ftype = import_object(ftype)

        self.paths.root = root or self.root
        self.paths.records = os.path.join(
            self.paths.root, StringUtils.snake(biz_class.__name__)
        )
        os.makedirs(self.paths.records, exist_ok=True)

        self._cache_store.bootstrap(biz_class.app)
        self._cache_store.bind(biz_class)
        self._cache_store.create_many(self.fetch_all(ignore_cache=True).values())

    def create_id(self, record):
        return record.get(ID_FIELD_NAME, UuidString.next_id())

    def exists(self, fname: Text) -> bool:
        return BaseFile.exists(self.mkpath(fname))

    def create(self, record: Dict) -> Dict:
        _id = self.create_id(record)
        record = self.update(_id, record)
        record[ID_FIELD_NAME] = _id
        self._cache_store.create(record)
        return record

    def create_many(self, records):
        created_records = []
        for record in records:
            created_records.append(self.create(record))
        self._cache_store.create_many(created_records)

    def count(self) -> int:
        fnames = glob.glob(f'{self.paths.records}/*.{self.extension}')
        return len(fnames)

    def fetch(self, _id, fields=None) -> Dict:
        records = self.fetch_many([_id], fields=fields)
        return records.get(_id) if records else {}

    def fetch_many(self, _ids: List, fields: List = None, ignore_cache=False) -> Dict:
        if not _ids:
            _ids = self._fetch_all_ids()

        if not ignore_cache:
            cached_records = self._cache_store.fetch_many(_ids)
            if cached_records:
                _ids -= cached_records.keys()
        else:
            cached_records = {}

        fields = fields if isinstance(fields, set) else set(fields or [])
        if not fields:
            fields = set(self.biz_class.Schema.fields.keys())
        fields |= {ID_FIELD_NAME, REV_FIELD_NAME}

        records = {}

        for _id in _ids:
            fpath = self.mkpath(_id)
            record = self.ftype.read(fpath)
            if record:
                record.setdefault(ID_FIELD_NAME, _id)
                record[REV_FIELD_NAME] = record.setdefault(REV_FIELD_NAME, 0)
                records[_id] = {k: record.get(k) for k in fields}
            else:
                records[ID_FIELD_NAME] = None

        self._cache_store.create_many(records.values())
        records.update(cached_records)
        return records

    def fetch_all(self, fields: Set[Text] = None, ignore_cache=False) -> Dict:
        return self.fetch_many(None, fields=fields, ignore_cache=ignore_cache)

    def update(self, _id, data: Dict) -> Dict:
        fpath = self.mkpath(_id)
        base_record = self.ftype.read(fpath)
        if base_record:
            # this is an upsert
            record = DictUtils.merge(base_record, data)
        else:
            record = data

        if ID_FIELD_NAME not in record:
            record[ID_FIELD_NAME] = _id

        if REV_FIELD_NAME not in record:
            record[REV_FIELD_NAME] = 0
        else:
            record[REV_FIELD_NAME] += 1

        self._cache_store.update(_id, record)
        self.ftype.write(path=fpath, data=record)
        return record

    def update_many(self, _ids: List, updates: List = None) -> Dict:
        return {
            _id: self.update(_id, data)
            for _id, data in zip(_ids, update)
        }

    def delete(self, _id) -> None:
        self._cache_store.delete(_id)
        fpath = self.mkpath(_id)
        os.unlink(fpath)

    def delete_many(self, _ids: List) -> None:
        for _id in _ids:
            self.delete(_id)

    def delete_all(self):
        _ids = self._fetch_all_ids()
        self.delete_many(_ids)

    def query(self, *args, **kwargs):
        return self._cache_store.query(*args, **kwargs)

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
