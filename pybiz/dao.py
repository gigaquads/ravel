from importlib import import_module
from abc import ABCMeta, abstractmethod


class DAOError(Exception):
    pass


class Dao(object, metaclass=ABCMeta):

    @abstractmethod
    def fetch(_id=None, public_id=None, fields: dict = None):
        pass

    @abstractmethod
    def fetch_many(_ids=None, public_ids=None, fields: dict = None):
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
        self._cached_dao_classes = {}

    def get_dao(self, bizobj_class):
        bizobj_class_name = bizobj_class.__name__

        # split the dotted path to the DAO class into a module
        # path and a class name for a DAO class inside of said module.
        dao_retval = bizobj_class.__dao__()
        if dao_retval is None:
            raise DAOError(
                '{} has no value for __dao__'.format(bizobj_class_name))
        elif not isinstance(dao_retval, str):
            dao_class = dao_retval
            return dao_class()

        class_path_str = bizobj_class.__dao__()
        if not class_path_str:
            raise DAOError(
                '{} has no DAO. ensure that the '
                '__dao__ classmethod returns '
                'the dotted path to the DAO class to use.'.format(
                    bizobj_class_name))

        class_path = class_path_str.split('.')
        assert len(class_path) > 1

        module_path_str = '.'.join(class_path[:-1])
        class_name = class_path[-1]

        # try first to fetch the DAO class from cache
        cache_key = class_path_str
        dao_class = self._cached_dao_classes.get(cache_key)
        if dao_class is not None:
            return dao_class()

        # otherwise, lazily load and cache the class
        # and return an instance.
        try:
            dao_module = import_module(module_path_str)
            dao_class = getattr(dao_module, class_name)
        except Exception as exc:
            raise DAOError(
                'failed to import {} when loading the DAO class '
                'specified by {}: {}.'.format(
                    class_path_str, bizobj_class_name, str(exc)))

        self._cached_dao_classes[cache_key] = dao_class
        return dao_class()
