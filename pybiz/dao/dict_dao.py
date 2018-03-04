from collections import defaultdict
from copy import deepcopy
import threading

from .base import Dao


class DictDao(Dao):

    _storage = defaultdict(lambda: defaultdict(dict))

    _local = threading.local()
    _local.id_counter = 1

    @classmethod
    def next_id(cls):
        next_id = cls._local.id_counter
        cls._local.id_counter += 1
        return next_id

    @classmethod
    def get_storage(cls):
        return cls._storage[cls.__name__]

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.storage = self.get_storage()

    def write(self, field: str, key, value):
        self.storage[field][key] = value

    def read(self, field: str, key):
        return self.storage[field].get(key)

    def exists(self, **kwargs) -> bool:
        return any(v in self.storage[k] for k, v in kwargs.items())

    def fetch(self, **kwargs) -> dict:
        fields = kwargs.pop('fields', None)
        for k, v in kwargs.items():
            record = self.storage[k].get(v)
            if record:
                return record
        return None

    def fetch_many(self, **kwargs) -> dict:
        kwargs.pop('fields', None)
        records = defaultdict(dict)
        for k, values in kwargs.items():
            for _k in values:
                record = self.storage[k].get(_k)
                if record:
                    records[k][_k] = record
        return records

    def create(self, _id=None, public_id=None, data: dict=None) -> dict:
        _id = _id or self.next_id()
        data['_id'] = _id
        self.storage['_id'][_id] = data
        if 'public_id' in data:
            self.storage['public_id'][data['public_id']] = data
        return data

    def update(self, _id=None, public_id=None, data: dict=None) -> dict:
        record = None
        if not record and _id:
            record = self.storage['_id'].get(_id)
        if not record and public_id:
            record = self.storage['public_id'].get(public_id)
        if record:
            record.update(deepcopy(data))
        return record

    def update_many(
        self, _ids: list=None, public_ids: list=None, data: list=None
    ) -> list:
        records = defaultdict(dict)
        data = deepcopy(data)
        for k, values in kwargs.items():
            for _k in values:
                record = self.storage[k].get(_k)
                record.update(data)
                records[k][_k] = record
        return records

    def delete(self, **kwargs) -> dict:
        results = defaultdict(dict)
        for k, v in kwargs:
            if v in self.storage[k]:
                results[k][v] = self.storage[k].pop(v)
        return results

    def delete_many(self, _ids: list=None, public_ids: list=None) -> list:
        records = defaultdict(dict)
        for k, values in kwargs.items():
            for _k in values:
                record = self.storage[k].pop(_k)
                records[k][_k] = record
        return records
