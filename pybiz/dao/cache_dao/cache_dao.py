from typing import Text, Type, List, Set, Dict, Tuple
from copy import deepcopy
from datetime import datetime

from ..base import Dao
from .cache_record import CacheRecord
from .cache_interface import CacheInterface


class CacheDao(Dao):
    def __init__(
        self,
        persistence: CacheInterface,
        cache: CacheInterface = None,
        *args, **kwargs
    ):
        from pybiz.dao.dict_dao import DictDao

        super().__init__(*args, **kwargs)

        self.persistence = persistence
        self.cache = cache or DictDao()

    def bind(self, bizobj_type):
        super().bind(bizobj_type)
        self.persistence.bind(bizobj_type)
        self.cache.bind(bizobj_type)

    def fetch(self, _id, fields: Dict = None) -> Dict:
        return self.fetch_many({_id}, fields=fields).get(_id)

    def fetch_many(self, _ids, fields: Dict = None) -> Dict:
        cached = self.cache.fetch_cache(_ids, data=True, fields=fields)
        latest = self.persistence.fetch_cache(_ids, data=False)

        ids_to_create = set(latest.keys() - cached.keys())
        ids_to_update = set()
        ids_to_purge = set()

        records = {}  # return value, Dict[_id, Dict]

        for _id, cached_record in  cached.items():
            latest_record = latest.get(_id)
            if latest_record and latest_record.rev is not None:
                if cached_record.rev < latest_record.rev:
                    # the cached record is stale, so collect
                    # the _id and fetch from persistence, below.
                    ids_to_update.add(_id)
                elif fields:
                    # the cached record is up-to-date, so use it
                    records[_id] = deepcopy({
                        k: cached_record.data[k] for k in fields
                    })
                else:
                    # the cached record is up-to-date, so use it
                    records[_id] = deepcopy(cached_record.data)
            else:
                # the requested record apparently doesn't
                # exist in persistence, so collect its _ids
                # to remove from cache, below.
                ids_to_purge.add(_id)

        # purge cache of any records that do not
        # exist in persistence anymore
        if ids_to_purge:
            self.cache.delete_many(ids_to_purge)

        # fetch the records not in cache
        fetched_records = self.persistence.fetch_many(
            _ids=(ids_to_create | ids_to_update),
            fields=fields
        )

        # upsert records to cache
        if ids_to_create:
            self.cache.create_many(
                records=[fetched_records[_id] for _id in ids_to_create]
            )
        if ids_to_update:
            self.cache.update_many(
                _ids=ids_to_update,
                data=[fetched_records[_id] for _id in ids_to_update]
            )

        records.update(fetched_records)
        return records

    def query(self, predicate, **kwargs):
        """
        """
        raise NotImplementedError('TODO')

    def exists(self, _id) -> bool:
        """
        Return True if the record with the given _id exists.
        """
        return self.persistence.exists(_id)

    def create(self, data: Dict) -> Dict:
        """
        Create a new record with the _id. If the _id is contained is not
        contained in the data dict nor provided as the _id argument, it is the
        responsibility of the Dao class to generate the _id.
        """
        record = self.persistence.create(data)
        self.cache.create(record)
        return record

    def create_many(self, records: List[Dict]) -> None:
        """
        Create a new record.  It is the responsibility of the Dao class to
        generate the _id.
        """
        self.persistence.create_many(records)
        self.cache.create(records)

    def update(self, _id, data: Dict) -> Dict:
        """
        Update a record with the data passed in.
        """
        record = self.persistence.update(_id, data)
        self.cache.update(_id, record)
        return record

    def update_many(self, _ids: List, data: List[Dict] = None) -> None:
        """
        Update multiple records. If a single data dict is passed in, then try to
        apply the same update to all records; otherwise, if a list of data dicts
        is passed in, try to zip the _ids with the data dicts and apply each
        unique update or each group of identical updates individually.
        """
        self.persistence.update_many(_ids, records)
        self.cache.update_many(_ids, records)

    def delete(self, _id) -> None:
        """
        Delete a single record.
        """
        self.persistence.delete(_id)
        self.cache.update(_id)

    def delete_many(self, _ids: List) -> None:
        """
        Delete multiple records.
        """
        self.persistence.delete_many(_ids)
        self.cache.delete_many(_ids)
