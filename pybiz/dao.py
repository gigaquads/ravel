import venusian

from importlib import import_module
from abc import ABCMeta, abstractmethod


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
    def exists(self, _id=None, public_id=None):
        pass

    @abstractmethod
    def fetch(self, _id=None, public_id=None, fields: dict = None):
        pass

    @abstractmethod
    def fetch_many(self, _ids=None, public_ids=None, fields: dict = None):
        pass

    @abstractmethod
    def create(self, data):
        pass  # should return a new _id

    @abstractmethod
    def save(self, _id, data: dict):
        pass  # should return the _id

    @abstractmethod
    def delete(self, _id):
        pass

    @abstractmethod
    def delete_many(self, _ids):
        pass


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
