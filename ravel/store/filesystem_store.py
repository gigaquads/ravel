import os
import glob

from typing import Text, List, Set, Dict, Tuple
from collections import defaultdict
from datetime import datetime
from threading import RLock

from appyratus.env import Environment
from appyratus.files import BaseFile, Yaml
from appyratus.schema.fields import UuidString
from appyratus.utils import (
    DictObject,
    DictUtils,
    StringUtils,
)

from ravel.util.misc_functions import import_object
from ravel.util.loggers import console
from ravel.constants import ID, REV
from ravel.exceptions import RavelError

from .base import Store
from .simulation_store import SimulationStore


class StoreError(RavelError):
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
        self._table_lock = RLock()
        self._row_locks = defaultdict(RLock)

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
    def on_bootstrap(
        cls, ftype: Text = None, root: Text = None, use_recursive_merge=True
    ):
        cls.ftype = import_object(ftype) if ftype else Yaml
        cls.root = root or cls.root
        cls.use_recursive_merge = use_recursive_merge
        if not cls.root:
            raise MissingBootstrapParameterError('missing parameter: root')

    def on_bind(self, resource_type, root: Text = None, ftype: BaseFile = None):
        """
        Ensure the data dir exists for this Resource type.
        """
        if isinstance(ftype, str):
            self.ftype = import_object(ftype)

        self.paths.root = root or self.root
        self.paths.records = os.path.join(
            self.paths.root, StringUtils.snake(resource_type.__name__)
        )
        os.makedirs(self.paths.records, exist_ok=True)

        # bootstrap, bind, and backfill the in-memory cache
        with self._table_lock:
            if not self._cache_store.is_bootstrapped():
                self._cache_store.bootstrap(resource_type.ravel.app)

            self._cache_store.bind(resource_type)
            self._cache_store.create_many(
                record for record in self.fetch_all(ignore_cache=True).values() if record
            )

    def create_id(self, record):
        return record.get(ID, UuidString.next_id())

    def exists(self, _id: Text) -> bool:
        return BaseFile.exists(self.mkpath(_id))

    def exists_many(self, _ids: Set) -> Dict[object, bool]:
        return {_id: self.exists(_id) for _id in _ids}

    def create(self, record: Dict) -> Dict:
        _id = self.create_id(record)
        with self._row_locks[_id]:
            record = self.update(_id, record)
            record[ID] = _id
            self._cache_store.create(record)
            return record

    def create_many(self, records):
        created_records = []
        for record in records:
            created_records.append(self.create(record))
        self._cache_store.create_many(created_records)
        return created_records

    def count(self) -> int:
        fnames = glob.glob(f'{self.paths.records}/*.{self.extension}')
        return len(fnames)

    def fetch(self, _id, fields=None) -> Dict:
        records = self.fetch_many([_id], fields=fields)
        record = records.get(_id) if records else {}
        return record

    def fetch_many(
        self,
        _ids: List = None,
        fields: Set[Text] = None,
        ignore_cache=False
    ) -> Dict:
        """
        """
        if not _ids:
            _ids = []

        # reduce _ids to its unique members by making it a set
        if not isinstance(_ids, set):
            all_ids = set(_ids)
        else:
            all_ids = _ids

        ids_to_fetch_from_fs = set()

        # we do not want to ignore the cache here
        if not ignore_cache:
            cached_records = self._cache_store.fetch_many(all_ids, fields=fields)
            for record_id, record in cached_records.items():
                if record is None:
                    ids_to_fetch_from_fs.add(record_id)
        # otherwise we will go straight to the filesystem
        else:
            cached_records = {}
            ids_to_fetch_from_fs = self._fetch_all_ids()

        # if there are any remaining ID's not returned from cache,
        # fetch them from file system
        if ids_to_fetch_from_fs:

            # prepare the set of field names to fetch
            fields = fields if isinstance(fields, set) else set(fields or [])
            if not fields:
                fields = set(self.resource_type.Schema.fields.keys())
            fields |= {ID, REV}

            records = {}
            non_null_records = []

            for _id in ids_to_fetch_from_fs:
                fpath = self.mkpath(_id)
                with self._row_locks[_id]:
                    try:
                        record = self.ftype.read(fpath)
                    except FileNotFoundError:
                        records[_id] = None
                        console.debug(
                            message='file not found by filesystem store',
                            data={'filepath': fpath}
                        )
                        continue

                if record:
                    record, errors = self.schema.process(record)
                    if errors:
                        raise Exception(
                            f'validation error while loading '
                            f'{_id}.{self.extension}'
                        )
                    record.setdefault(ID, _id)
                    records[_id] = {k: record.get(k) for k in fields}

                    non_null_records.append(record)

                    # if for some reason a file was created manually
                    # with a _rev, we create one here and save it
                    if REV not in record:
                        record[REV] = self.increment_rev()
                        self.update(_id, record)
                else:
                    records[ID] = None

            self._cache_store.create_many(non_null_records)
            cached_records.update(records)

        return cached_records

    def fetch_all(self, fields: Set[Text] = None, ignore_cache=False) -> Dict:
        return self.fetch_many(None, fields=fields, ignore_cache=ignore_cache)

    def update(self, _id, data: Dict) -> Dict:
        fpath = self.mkpath(_id)
        with self._row_locks[_id]:
            base_record = self.ftype.read(fpath)

        schema = self.resource_type.ravel.schema
        base_record, errors = schema.process(base_record)

        if base_record:
            # this is an upsert
            if self.use_recursive_merge:
                record = DictUtils.merge(base_record, data)
            else:
                record = dict(base_record, **data)
        else:
            record = data

        record[REV] = self.increment_rev(record.get(REV))
        if ID not in record:
            record[ID] = _id

        with self._row_locks[_id]:
            self.ftype.write(path=fpath, data=record)

        self._cache_store.update(_id, record)
        return record

    def update_many(self, _ids: List, updates: List = None) -> Dict:
        return {
            _id: self.update(_id, data)
            for _id, data in zip(_ids, updates)
        }

    def delete(self, _id) -> None:
        self._cache_store.delete(_id)
        fpath = self.mkpath(_id)
        with self._row_locks[_id]:
            os.remove(fpath)

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
