from collections import defaultdict
from copy import deepcopy
from threading import RLock

from .base import Dao


class DictDao(Dao):
    """
    This is a simple Dao that stores records in a plain ol' dict. Complex
    queries are not implemented, just _id-based CRUD methods. Nothing is
    persisted to disk. It is ephemeral, purely in memory.
    """

    _id_counter = 1
    _id_counter_lock = RLock()

    _id_2_record = {}
    _id_2_record_lock = RLock()

    @classmethod
    def next_id(cls):
        with cls._id_counter_lock:
            next_id = cls._id_counter
            cls._id_counter += 1
            return next_id

    def exists(self, _id) -> bool:
        with self._id_2_record_lock:
            return _id in self._id_2_record

    def fetch(self, _id, fields=None) -> dict:
        record = None
        with self._id_2_record_lock:
            record = self._id_2_record.get(_id)
            if record:
                fields = fields or record.keys()
                record = {k: v for k, v in record.items() if k in fields}
        return record

    def fetch_many(self, _ids, fields=None) -> list:
        with self._id_2_record_lock:
            return [
                self.fetch(_id=_id, fields=fields)
                for _id in _ids
            ]

    def create(self, _id, record: dict=None) -> dict:
        _id = _id or self.next_id()
        record['_id'] = _id
        with self._id_2_record_lock:
            self._id_2_record[_id] = record
        return record

    def update(self, _id=None, data: dict=None) -> dict:
        record = None
        with self._id_2_record_lock:
            record = self._id_2_record[_id]
            record.update(data)
            with self._id_2_record_lock:
                self._id_2_record[record['_id']] = record
        return record

    def update_many(self, _ids: list, data: list=None) -> list:
        return [
            self.update(_id=_id, data=data_dict)
            for _id, data_dict in zip(_ids, data)
        ]

    def delete(self, _id) -> dict:
        with self._id_2_record_lock:
            return self._id_2_record.pop(_id, None)

    def delete_many(self, _ids: list) -> list:
        with self._id_2_record_lock:
            return [self._id_2_record.pop(_id, None) for _id in _ids]

    def query(self, predicate, **kwargs):
        raise NotImplementedError('')
