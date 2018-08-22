import os

import venusian

from typing import Dict, List
from abc import ABCMeta, abstractmethod


class DaoMeta(ABCMeta):
    def __init__(cls, name, bases, dict_):
        ABCMeta.__init__(cls, name, bases, dict_)

        def callback(scanner, name, dao_class):
            scanner.bizobj_classes[name] = dao_class

        venusian.attach(cls, callback, category='dao')


class Dao(object, metaclass=DaoMeta):
    @abstractmethod
    def query(self, predicate, **kwargs):
        pass

    @abstractmethod
    def exists(self, _id) -> bool:
        """
        Return True if the record with the given _id exists.
        """

    @abstractmethod
    def fetch(self, _id, fields: Dict=None) -> Dict:
        """
        Read a single record by _id, selecting only the designated fields (or
        all by default).
        """

    @abstractmethod
    def fetch_many(self, _ids: List, fields: Dict=None) -> Dict:
        """
        Read multiple records by _id, selecting only the designated fields (or
        all by default).
        """

    @abstractmethod
    def create(self, _id, data: Dict) -> Dict:
        """
        Create a new record with the _id. If the _id is contained is not
        contained in the data dict nor provided as the _id argument, it is the
        responsibility of the Dao class to generate the _id.
        """

    @abstractmethod
    def update(self, _id, data: Dict) -> Dict:
        """
        Update a record with the data passed in.
        """

    @abstractmethod
    def update_many(self, _ids: List, data: List[Dict]=None) -> List:
        """
        Update multiple records. If a single data dict is passed in, then try to
        apply the same update to all records; otherwise, if a list of data dicts
        is passed in, try to zip the _ids with the data dicts and apply each
        unique update or each group of identical updates individually.
        """

    @abstractmethod
    def delete(self, _id) -> Dict:
        """
        Delete a single record, returning the record if possible.
        """

    @abstractmethod
    def delete_many(self, _ids: list) -> Dict:
        """
        Delete multiple records, returning a map from _id to an implementation
        defined return value.
        """


class DaoManager(object):
    """
    Stores and manages a global registry, entailing which BizObject class is
    associated with which Dao class.
    """

    _instance = None  # the singleton instance

    @classmethod
    def get_instance(cls):
        """
        Get global DaoManager singleton instance.
        """
        if cls._instance is None:
            cls._instance = DaoManager()
        return cls._instance

    def __init__(self):
        self._bizobj_type_2_dao_type = {}  # i.e. BizObject => Dao

    def register(self, bizobj_class, dao_class):
        self._bizobj_type_2_dao_type[bizobj_class] = dao_class

    def get_dao(self, bizobj_class) -> Dao:
        if bizobj_class not in self._bizobj_type_2_dao_type:
            raise KeyError(
                'Unable to find "{}" in dao classes. '
                'Hint: did you create a manifest file?'.format(
                    bizobj_class.__name__
                )
            )
        dao_class = self._bizobj_type_2_dao_type[bizobj_class]
        return dao_class()
