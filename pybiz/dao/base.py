import os
import venusian

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
    def query(self, predicate, **kwargs):
        raise NotImplementedError()

    @abstractmethod
    def exists(self, _id=None, public_id=None) -> bool:
        pass

    @abstractmethod
    def fetch(self, _id=None, public_id=None, fields: dict=None) -> dict:
        pass

    @abstractmethod
    def fetch_many(
        self, _ids: list=None, public_ids: list=None, fields: dict=None
    ) -> dict:
        pass

    @abstractmethod
    def create(self, _id=None, public_id=None, data: dict=None) -> dict:
        pass

    @abstractmethod
    def update(self, _id=None, public_id=None, data: dict=None) -> dict:
        pass

    @abstractmethod
    def update_many(
        self, _ids: list=None, public_ids: list=None, data: list=None
    ) -> dict:
        pass

    @abstractmethod
    def delete(self, _id=None, public_id=None) -> dict:
        pass

    @abstractmethod
    def delete_many(self, _ids: list=None, public_ids: list=None) -> dict:
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
        if bizobj_class not in self._dao_classes:
            raise Exception(
                'Unable to find "{}" in dao classes.  Hint: did you define a manifest?'.
                format(bizobj_class.__name__)
            )
        dao_class = self._dao_classes[bizobj_class]
        return dao_class()
