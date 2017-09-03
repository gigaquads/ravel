import venusian

from abc import ABCMeta, abstractmethod
from importlib import import_module
from collections import defaultdict
from copy import deepcopy


class DAOError(Exception):
    pass


class DaoMeta(ABCMeta):

    def __init__(cls, name, bases, dict_):
        ABCMeta.__init__(cls, name, bases, dict_)

        def callback(scanner, name, dao_class):
            scanner.dao_classes[name] = dao_class

        venusian.attach(cls, callback, category='dao')


class Dao(object, metaclass=DaoMeta):

    @abstractmethod
    def exists(self, _id=None, public_id=None) -> bool:
        pass

    @abstractmethod
    def fetch(self, _id=None, public_id=None, fields: dict=None) -> dict:
        pass

    @abstractmethod
    def fetch_many(self, _ids: list=None, public_ids:list=None, fields: dict=None) -> dict:
        pass

    @abstractmethod
    def create(self, _id=None, public_id=None, data: dict=None) -> dict:
        pass

    @abstractmethod
    def update(self, _id=None, public_id=None, data: dict=None) -> dict:
        pass

    @abstractmethod
    def update_many(self, _ids: list=None, public_ids: list=None, data: list=None) -> dict:
        pass

    @abstractmethod
    def delete(self, _id=None, public_id=None) -> dict:
        pass

    @abstractmethod
    def delete_many(self, _ids: list=None, public_ids: list=None) -> dict:
        pass


class DictDao(Dao):

    _data = defaultdict(dict)

    def write(self, field: str, key, value):
        self._data[field][key] = value

    def read(self, field: str, key):
        return self._data[field].get(key)

    def exists(self, **kwargs) -> bool:
        return any(v in self._data[k] for k, v in kwargs.items())

    def fetch(self, **kwargs) -> dict:
        kwargs.pop('fields', None)
        for k, v in kwargs.items():
            record = self._data[k].get(v)
            if record:
                return record

    def fetch_many(self, **kwargs) -> dict:
        kwargs.pop('fields', None)
        records = defaultdict(dict)
        for k, values in kwargs.items():
            for _k in values:
                record = self._data[k].get(_k)
                if record:
                    records[k][_k] = record
        return records

    def create(self, _id=None, public_id=None, data: dict=None) -> dict:
        if _id:
            data['_id'] = _id
            self._data['_id'] = data
        if public_id:
            data['public_id'] = public_id
            self._data['public_id'] = data
        return data

    def update(self, _id=None, public_id=None, data: dict=None) -> dict:
        record = None
        if not record and _id:
            record = self._data.get(_id)
        if not record and public_id:
            record = self._data.get(public_id)
        if record:
            record.update(deepcopy(data))
        return record

    def update_many(self, _ids: list=None, public_ids: list=None, data: list=None) -> list:
        records = defaultdict(dict)
        for k, values in kwargs.items():
            for _k in values:
                record = self._data[k].get(_k)
                record.update(deepcopy(data))
                records[k][_k] = record
        return records

    def delete(self, **kwargs) -> dict:
        results = defaultdict(dict)
        for k, v in kwargs:
            if v in self._data[k]:
                results[k][v] = self._data[k].pop(v)
        return results

    def delete_many(self, _ids: list=None, public_ids: list=None) -> list:
        records = defaultdict(dict)
        for k, values in kwargs.items():
            for _k in values:
                record = self._data[k].pop(_k)
                records[k][_k] = record
        return records


class DaoManager(object):

    _instance = None

    @classmethod
    def get_instance(cls):
        if cls._instance is None:
            cls._instance = DaoManager()
        return cls._instance

    def __init__(self):
        self._dao_classes = {}

    def register(self, bizobj_class, dao_class):
        self._dao_classes[bizobj_class] = dao_class

    def get_dao(self, bizobj_class):
        dao_class = self._dao_classes[bizobj_class]
        return dao_class()
