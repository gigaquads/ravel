from collections import defaultdict
from copy import deepcopy
from threading import RLock

from .base import Dao


class DictDao(Dao):

    _id_counter = 1
    _id_counter_lock = RLock()

    _id_2_record = {}
    _id_2_record_lock = RLock()

    _public_id_2_record = {}
    _public_id_2_record_lock = RLock()

    @classmethod
    def next_id(cls):
        with cls._id_counter_lock:
            next_id = cls._id_counter
            cls._id_counter += 1
            return next_id

    def exists(self, _id=None, public_id=None) -> bool:
        if _id is not None:
            with self._id_2_record_lock:
                return _id in self._id_2_record

        if public_id is not None:
            with self._public_id_2_record_lock:
                return public_id in self._public_id_2_record

    def fetch(self, _id=None, public_id=None, fields=None) -> dict:
        record = None

        if _id is not None:
            with self._id_2_record_lock:
                record = self._id_2_record.get(_id)
        else:
            with self._public_id_2_record_lock:
                record = self._public_id_2_record.get(public_id)

        if record:
            fields = fields or record.keys()
            record = {k: v for k, v in record.items() if k in fields}

        return record

    def fetch_many(self, _ids=None, public_ids=None, fields=None) -> list:
        if _ids is not None:
            with self._id_2_record_lock:
                return [
                    self.fetch(_id=_id, fields=fields)
                    for _id in _ids
                ]
        if public_ids is not None:
            with self._public_id_2_record_lock:
                return [
                    self.fetch(public_id=public_id, fields=fields)
                    for public_id in public_ids
                ]

    def create(self, _id=None, public_id=None, record: dict=None) -> dict:
        _id = _id or self.next_id()
        public_id = public_id or record.get('public_id')

        record['_id'] = _id
        with self._id_2_record_lock:
            self._id_2_record[_id] = record

        if public_id:
            record['public_id'] = public_id
            with self._public_id_2_record_lock:
                self._public_id_2_record[public_id] = record

        return record

    def update(self, _id=None, public_id=None, data: dict=None) -> dict:
        record = None

        if _id:
            with self._id_2_record_lock:
                record = self._id_2_record[_id]
                record.update(data)
                with self._public_id_2_record_lock:
                    self._public_id_2_record[record['public_id']] = record
        elif public_id:
            with self._public_id_2_record_lock:
                record = self._public_id_2_record[public_id]
                record.update(data)
                with self._id_2_record_lock:
                    self._public_id_2_record[record['_id']] = record

        return record

    def update_many(
        self, _ids: list=None, public_ids: list=None, data: list=None
    ) -> list:
        if _ids is not None:
            return [
                self.update(_id=_id, data=data_dict)
                for _id, data_dict in zip(_ids, data)
            ]
        elif public_ids is not None:
            return [
                self.update(public_id=public_id, data=data_dict)
                for public_id, data_dict in zip(public_ids, data)
            ]
        else:
            return []

    def delete(self, _id=None, public_id=None) -> dict:
        if _id is not None:
            with self._id_2_record_lock:
                self._id_2_record.pop(_id, None)
        else:
            with self._public_id_2_record_lock:
                self._public_id_2_record.pop(public_id, None)

    def delete_many(self, _ids: list=None, public_ids: list=None) -> list:
        if _ids is not None:
            with self._id_2_record_lock:
                return [self._id_2_record.pop(_id, None) for _id in _ids]
        elif public_ids is not None:
            with self._public_id_2_record_lock:
                return [
                    self._public_id_2_record.pop(public_id, None)
                    for public_id in public_ids
                ]
        else:
            return []
