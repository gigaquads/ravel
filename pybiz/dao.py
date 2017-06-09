from importlib import import_module
from abc import ABCMeta, abstractmethod


# TODO: make BizObject.dao_provider simply return the dotted path to the DAO

# class to use and scrap the DaoManager

class DAOError(Exception):
    pass


class Dao(object, metaclass=ABCMeta):

    @abstractmethod
    def fetch(_id, fields: dict = None):
        pass

    @abstractmethod
    def fetch_many(_ids, fields: dict = None):
        pass

    @abstractmethod
    def create(self, data):
        pass

    @abstractmethod
    def save(self, _id, data: dict):
        pass

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
        """
        self._factories has the following format:
            {
                'dao_provider': {'bizobj_class_name': dao_class}
            }

        For example:
            {
                'postgres': {'User': UserPostgresDAO},
                'mysql': {'User': UserMysqlDAO},
            }
        """
        self._factories = {}

    def register(self, provider: str, class_map: dict):
        provider = provider.lower()
        if provider not in self._factories:
            self._factories[provider] = class_map
        else:
            self._factories[provider].update(class_map)

    def get_dao_for_bizobj(self, bizobj_class):
        bizobj_class_name = bizobj_class.__name__

        # split the dotted path to the DAO class into a module
        # path and a class name for a DAO class inside of said module.
        class_path_str = bizobj_class.get_dotted_dao_class_path()
        if not class_path_str:
            raise DAOError(
                '{} has no DAO. ensure that the '
                'get_dotted_dao_class_path classmethod returns '
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
            return dao_class

        # otherwise, lazily load and cache the class
        # and return an instance.
        try:
            dao_module = import_module(module_path_str)
            dao_class = getattr(dao_module, cache_key)
        except Exception as exc:
            raise DAOError(
                'failed to import {} when loading the DAO class '
                'specified by {}: {}.'.format(
                    class_path_str, bizobj_class_name, exc.message))

        return dao_class()
