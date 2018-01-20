import os
import threading
import venusian

from abc import ABCMeta, abstractmethod
from importlib import import_module
from collections import defaultdict
from copy import deepcopy

from appyratus.yaml import Yaml


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
    def fetch_many(self,
                   _ids: list=None,
                   public_ids: list=None,
                   fields: dict=None) -> dict:
        pass

    @abstractmethod
    def create(self, _id=None, public_id=None, data: dict=None) -> dict:
        pass

    @abstractmethod
    def update(self, _id=None, public_id=None, data: dict=None) -> dict:
        pass

    @abstractmethod
    def update_many(self,
                    _ids: list=None,
                    public_ids: list=None,
                    data: list=None) -> dict:
        pass

    @abstractmethod
    def delete(self, _id=None, public_id=None) -> dict:
        pass

    @abstractmethod
    def delete_many(self, _ids: list=None, public_ids: list=None) -> dict:
        pass


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

    def create(self, _id=None, data: dict=None) -> dict:
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

    def update_many(self,
                    _ids: list=None,
                    public_ids: list=None,
                    data: list=None) -> list:
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


class YamlDao(DictDao):
    @classmethod
    def read_all(cls):
        profile_data = []
        for root, dirs, files in os.walk(self.data_path):
            for yamlfile in files:
                filepath = os.path.join(root, yamlfile)
                with open(filepath) as yamlraw:
                    res = yaml.load(yamlraw.read())
                    profile_data.append(res)
        return profile_data

    @classmethod
    def file_path(cls, key):
        return os.path.join(cls.data_path, Yaml.format_file_name(key))

    def exists(self, _id=None, public_id=None) -> bool:
        raise NotImplementedError('override `exists` in subclass')

    def fetch(self, _id=None, public_id=None, fields: dict=None) -> dict:
        data = super().fetch(_id=_id, public_id=public_id, fields=fields)
        if not data:
            data = Yaml.from_file(self.file_path(_id or public_id))
        return data

    def fetch_many(self,
                   _ids: list=None,
                   public_ids: list=None,
                   fields: dict=None) -> dict:
        raise NotImplementedError('override in subclass')

    def create(self, _id=None, public_id=None, data: dict=None) -> dict:
        raise NotImplementedError('override `create` in subclass')

    def update(self, _id=None, public_id=None, data: dict=None) -> dict:
        data = super().update(_id=_id, public_id=public_id, data=data)

        import ipdb
        ipdb.set_trace()
        pass

    def update_many(self,
                    _ids: list=None,
                    public_ids: list=None,
                    data: list=None) -> dict:
        raise NotImplementedError('override in subclass')

    def delete(self, _id=None, public_id=None) -> dict:
        raise NotImplementedError('override in subclass')

    def delete_many(self, _ids: list=None, public_ids: list=None) -> dict:
        raise NotImplementedError('override in subclass')


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
