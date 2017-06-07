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
        provider = bizobj_class.dao_provider().lower()
        class_name = bizobj_class.__name__
        if provider is None:
            raise DAOError('{} has no provider'.format(class_name))

        factory = self._factories.get(provider.lower())
        if factory is None:
            raise DAOError('{} provider not recognized'.format(provider))

        dao_class = factory.get(class_name)
        if dao_class is None:
            raise DAOError('{} not registered'.format(class_name))

        return dao_class()
