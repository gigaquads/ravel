from typing import Text, Type, List, Set, Dict, Tuple
from copy import deepcopy
from datetime import datetime

from appyratus.enum import EnumValueStr

from pybiz.util import remove_keys

from .base import Dao


class CacheMode(EnumValueStr):

    @staticmethod
    def values():
        return {'writethru', 'readonly'}



class CacheDao(Dao):
    def __init__(
        self,
        backend: Dao,
        frontend: Dao = None,
        prefetch=False,
        mode='writethru'
    ):
        from pybiz.dao.dict_dao import DictDao

        super().__init__()

        self.be = backend
        self.fe = frontend or DictDao()
        self.fe.ignore_rev = True

        self.prefetch = prefetch
        self.mode = mode

    def bind(self, biz_type):
        super().bind(biz_type)
        """
        if self.prefetch:
            records = self.persistence.fetch_cache(None, data=True, rev=True)
            self.cache.create_many(records=(r.data for r in records.values()))
        """
        self.be.bind(biz_type)
        self.fe.bind(biz_type)
        if self.prefetch:
            self.fetch_all()

    def create_id(self, record):
        raise NotImplementedError()

    def count(self) -> int:
        return self.be.count()

    def fetch(self, _id, fields: Dict = None) -> Dict:
        return self.fetch_many({_id}, fields=fields).get(_id)

    def fetch_all(self, fields: Set[Text] = None) -> Dict:
        be_ids = {
            rec['_id'] for rec in
            self.be.fetch_all(fields={'_id'}).values()
        }
        return self.fetch_many(be_ids, fields=fields)

    def fetch_many(self, _ids, fields: Dict = None) -> Dict:
        fe_records = self.fe.fetch_many(_ids, fields=fields)
        be_revs = self.be.fetch_many(fe_records.keys(), fields={'_rev'})

        ids = set(_ids) if not isinstance(_ids, set) else _ids
        ids_fe = set(fe_records.keys())                 # ids in FE
        ids_missing = ids - ids_fe                      # ids not in FE
        ids_to_delete = ids_fe - be_revs.keys()         # ids to delete in FE
        ids_to_update = {                               # ids to update in FE
            _id for _id, fe_rec in fe_records.items()
            if be_revs.get(_id, {}).get('_rev', 0) > fe_rec.get('_rev', 0)
        }

        # records in BE ONLY
        ids_to_fetch_from_be = ids_missing | ids_to_update
        if ids_to_fetch_from_be:
            be_records = self.be.fetch_many(ids_to_fetch_from_be)
        else:
            be_records = {}

        # partition fe_records into separate lists for
        # performing batch insert and update
        records_to_update = []
        records_to_create = []
        for _id, be_rec in be_records.items():
            if _id in ids_missing:
                records_to_create.append(be_rec)
            elif _id in ids_to_update:
                records_to_update.append(be_rec)

        # perform batch operations in FE
        if ids_to_delete:
            self.fe.delete_many(ids_to_delete)
        if records_to_create:
            self.fe.create_many(records_to_create)
        if records_to_update:
            self.fe.update_many(
                (rec['_id'] for rec in records_to_update),
                records_to_update
            )

        # merge fresh BE records to return into FE records
        if be_records:
            # TODO: prune the be_records to fields
            if fields:
                all_fields = set(self.biz_type.schema.fields.keys())
                fields_to_remove = all_fields - fields
                for be_rec in remove_keys(
                    be_records.values(), fields_to_remove, in_place=True
                ):
                    fe_records[be_rec['_id']] = be_rec
            else:
                fe_records.update(be_records)

        return fe_records

    def query(self, predicate, **kwargs):
        """
        """
        fe_records = self.fe.query(predicate=predicate, **kwargs)
        ids_fe = {rec['_id'] for rec in fe_records}

        # TODO: update predicate to fetch records with stale revs too
        predicate = self.biz_type._id.excluding(ids_fe) & predicate
        be_records = self.be.query(predicate=predicate, **kwargs)

        # do batch FE operations
        self.fe.create_many(be_records)

        # merge BE records into FE records to return
        fe_records.extend(be_records)

        return fe_records

    def exists(self, _id) -> bool:
        """
        Return True if the record with the given _id exists.
        return self.persistence.exists(_id)
        """

    def create(self, data: Dict) -> Dict:
        """
        Create a new record with the _id. If the _id is contained is not
        contained in the data dict nor provided as the _id argument, it is the
        responsibility of the Dao class to generate the _id.
        """
        be_record = self.be.create(data)
        fe_record = self.fe.create(be_record)
        return fe_record

    def create_many(self, records: List[Dict]) -> None:
        """
        Create a new record.  It is the responsibility of the Dao class to
        generate the _id.
        """
        if self.mode == CaheMode.writethru:
            be_records = self.be.create_many(records)
        else:
            be_records = records

        fe_records = self.cache.create(be_records.values())
        return fe_records

    def update(self, _id, record: Dict) -> Dict:
        """
        Update a record with the data passed in.
        record = self.persistence.update(_id, data)
        """
        if self.mode == CacheMode.writethru:
            be_record = self.be.update(_id, record)
        else:
            be_record = record

        # upsert into FE
        if self.fe.exists(_id):
            fe_record = self.fe.update(_id, be_record)
        else:
            fe_record = self.fe.create(be_record)

        return fe_record

    def update_many(self, _ids: List, data: List[Dict] = None) -> None:
        """
        Update multiple records. If a single data dict is passed in, then try to
        apply the same update to all records; otherwise, if a list of data dicts
        is passed in, try to zip the _ids with the data dicts and apply each
        unique update or each group of identical updates individually.
        """
        #records = self.be.update_many(_ids, records)
        # TODO: upsert in FE

    def delete(self, _id) -> None:
        """
        Delete a single record.
        """
        if self.mode == CacheMode.writethru:
            self.be.delete(_id)

        self.fe.delete(_id)

    def delete_many(self, _ids: List) -> None:
        """
        Delete multiple records.
        """
        if self.mode == CacheMode.writethru:
            self.be.delete_many(_ids)

        self.fe.delete_many(_ids)
