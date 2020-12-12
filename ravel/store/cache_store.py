import os
import multiprocessing as mp

from typing import Text, Type, List, Set, Dict, Tuple
from copy import deepcopy
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor

from appyratus.enum import EnumValueStr

from ravel.util.misc_functions import remove_keys, import_object
from ravel.constants import ID, REV

from .base import Store, StoreEvent


class CacheMode(EnumValueStr):

    @staticmethod
    def values():
        return {
            'writethru',
            'writeback',
            'readonly',
        }


class CacheStoreExecutor(ThreadPoolExecutor):

    def __init__(self, store: 'CacheStore'):
        super().__init__(max_workers=1, initializer=self.initializer)
        self.store = store

    def initializer(self):
        self.store.be.bootstrap(self.store.be.app)
        self.store.be.bind(self.store.be.resource_type)

    def enqueue(self, method: Text, args=None, kwargs=None):

        def task(store, event):
            store.play([event])

        event = StoreEvent(method=method, args=args, kwargs=kwargs)
        return self.submit(task, store=self.store.be, event=event)


class CacheStore(Store):

    prefetch = False
    mode = CacheMode.writethru
    fe = None
    be = None
    fe_params = None
    be_params = None

    def __init__(self):
        super().__init__()
        self.executor = None

    @classmethod
    def on_bootstrap(cls, prefetch=False, mode=None, front=None, back=None):
        from .simulation_store import SimulationStore

        cls.prefetch = prefetch if prefetch is not None else cls.prefetch
        cls.mode = mode or cls.mode
        cls.fe = SimulationStore()
        cls.fe_params = front
        cls.be_params = back

    def on_bind(
        self,
        resource_type: Type['Resource'],
        prefetch=False,
        mode: CacheMode = None,
        front: Dict = None,
        back: Dict = None,
    ):
        if prefetch is not None:
            self.prefetch = prefetch

        self.mode = mode or self.mode
        front = front or self.fe_params
        back = back or self.be_params

        self.fe = self._setup_inner_store(
            resource_type,
            front['store'],
            front.get('params', {}),
        )
        self.be = self._setup_inner_store(
            resource_type,
            back['store'],
            back.get('params', {}),
        )

        if self.prefetch:
            self.fetch_all()

        if self.mode == CacheMode.writeback:
            self.executor = CacheStoreExecutor(self)

    def _setup_inner_store(
        self, resource_type: Type['BizType'], store_class_name: Text, bind_params: Dict = None
    ):
        # fetch the store type from the ResourceBinder
        store_type = resource_type.ravel.app.manifest.store_classes.get(
            store_class_name.split('.')[-1]
        )
        if store_type is None:
            raise Exception(f'{store_type} not registered')

        # create an instance of this store and bind it
        store = store_type()
        if not store.is_bound:
            store.bind(resource_type, **(bind_params or {}))

        return store

    def create_id(self, record):
        raise NotImplementedError()

    def count(self) -> int:
        return self.be.count()

    def fetch(self, _id, fields: Dict = None) -> Dict:
        return self.fetch_many({_id}, fields=fields).get(_id)

    def fetch_all(self, fields: Set[Text] = None) -> Dict:
        be_ids = {
            rec[ID]
            for rec in self.be.fetch_all(fields={ID}).values()
            if rec is not None
        }
        return self.fetch_many(be_ids, fields=fields)

    def fetch_many(self, _ids, fields: Dict = None) -> Dict:
        fe_records = self.fe.fetch_many(_ids, fields=fields)
        be_revs = self.be.fetch_many(fe_records.keys(), fields={REV})

        ids = set(_ids) if not isinstance(_ids, set) else _ids
        ids_fe = set(fe_records.keys())    # ids in FE
        ids_missing = ids - ids_fe    # ids not in FE
        ids_to_delete = ids_fe - be_revs.keys()    # ids to delete in FE
        ids_to_update = set()    # ids to update in FE

        for _id, fe_rec in fe_records.items():
            if fe_rec is None:
                ids_missing.add(_id)
            elif be_revs.get(_id, {}).get(REV, 0) > fe_rec.get(REV, 0):
                ids_to_update.add(_id)

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
                (rec[ID] for rec in records_to_update), records_to_update
            )

        # merge fresh BE records to return into FE records
        if be_records:
            # TODO: prune the be_records to fields
            if fields:
                all_fields = set(self.resource_type.Schema.fields.keys())
                fields_to_remove = all_fields - fields
                for be_rec in remove_keys(
                    be_records.values(), fields_to_remove, in_place=True
                ):
                    if be_rec:
                        fe_records[be_rec[ID]] = be_rec
            else:
                fe_records.update(be_records)

        return fe_records

    def query(self, predicate, **kwargs):
        """
        """
        fe_records = self.fe.query(predicate=predicate, **kwargs)
        ids_fe = {rec[ID] for rec in fe_records}

        # TODO: update predicate to fetch records with stale revs too
        predicate = self.resource_type._id.excluding(ids_fe) & predicate
        be_records = self.be.query(predicate=predicate, **kwargs)

        # do batch FE operations
        # merge BE records into FE records to return
        fe_records.extend(self.fe.create_many(be_records))

        return fe_records

    def exists(self, _id) -> bool:
        """
        Return True if the record with the given _id exists.
        """
        return self.be.exists(_id)

    def exists_many(self, _ids: Set) -> Dict[object, bool]:
        return self.be.exists_many(_ids)

    def create(self, data: Dict) -> Dict:
        """
        Create a new record with the _id. If the _id is contained is not
        contained in the data dict nor provided as the _id argument, it is the
        responsibility of the Store class to generate the _id.
        """
        fe_record = self.fe.create(data)

        # remove _rev from a copy of fe_record so that the BE store doesn't
        # increment it from what was set by the FE store.
        fe_record_no_rev = fe_record.copy()
        del fe_record_no_rev[REV]

        if self.mode == CacheMode.writethru:
            self.be.create(fe_record_no_rev)
        if self.mode == CacheMode.writeback:
            self.executor.enqueue('create', args=(fe_record_no_rev, ))

        return fe_record

    def create_many(self, records: List[Dict]) -> None:
        """
        Create a new record.  It is the responsibility of the Store class to
        generate the _id.
        """
        fe_records = self.fe.create_many(records)

        fe_records_no_rev = []
        for rec in fe_records.values():
            rec = rec.copy()
            del rec[REV]
            fe_records_no_rev.append(rec)

        if self.mode == CaheMode.writethru:
            be_records = self.be.create_many(fe_records_no_rev)
        elif self.mode == CacheMode.writeback:
            self.executor.enqueue('create_many', args=(fe_records_no_rev, ))

        return fe_records

    def update(self, _id, record: Dict) -> Dict:
        """
        Update a record with the data passed in.
        record = self.persistence.update(_id, data)
        """
        record.setdefault(ID, _id)

        if not self.fe.exists(_id):
            return self.create(record)

        fe_record = self.fe.update(_id, record)
        fe_record_no_rev = fe_record.copy()
        del fe_record_no_rev[REV]

        if self.mode == CacheMode.writethru:
            self.be.update(_id, fe_record_no_rev)
        elif self.mode == CacheMode.writeback:
            self.executor.enqueue(
                'update', args=(
                    _id,
                    fe_record_no_rev,
                )
            )

        return fe_record

    def update_many(self, _ids: List, data: Dict = None) -> None:
        """
        Update multiple records. If a single data dict is passed in, then try to
        apply the same update to all records; otherwise, if a list of data dicts
        is passed in, try to zip the _ids with the data dicts and apply each
        unique update or each group of identical updates individually.
        """
        fe_records = self.fe.update_many(records)

        if self.mode == CaheMode.writethru:
            be_records = self.be.update_many(_ids, records)
        elif self.mode == CacheMode.writeback:
            self.executor.enqueue(
                'create_many', args=(_ids, ), kwargs={'data': data}
            )
        return fe_records

    def delete(self, _id) -> None:
        """
        Delete a single record.
        """
        self.fe.delete(_id)

        if self.mode == CacheMode.writethru:
            self.be.delete(_id)
        elif self.mode == CacheMode.writeback:
            self.executor.enqueue('delete', args=(_id, ))

    def delete_many(self, _ids: List) -> None:
        """
        Delete multiple records.
        """
        self.fe.delete_many(_ids)

        if self.mode == CacheMode.writethru:
            self.be.delete_many(_ids)
        elif self.mode == CacheMode.writeback:
            self.executor.enqueue('delete_many', args=(_ids, ))

    def delete_all(self) -> None:
        raise NotImplementedError()
