import os
import venusian

from typing import Dict, List, Type
from abc import ABCMeta, abstractmethod


class DaoMeta(ABCMeta):
    def __init__(cls, name, bases, dict_):
        ABCMeta.__init__(cls, name, bases, dict_)

        def callback(scanner, name, dao_class):
            scanner.dao_classes[name] = dao_class

        venusian.attach(cls, callback, category='dao')


class Dao(object, metaclass=DaoMeta):

    # set by self.bind
    bizobj_type = None

    def bind(self, bizobj_type: Type['BizObject']):
        self.bizobj_type = bizobj_type

    @abstractmethod
    def query(self, predicate, **kwargs):
        pass

    @abstractmethod
    def exists(self, _id) -> bool:
        """
        Return True if the record with the given _id exists.
        """

    @abstractmethod
    def fetch(self, _id, fields: Dict = None) -> Dict:
        """
        Read a single record by _id, selecting only the designated fields (or
        all by default).
        """

    @abstractmethod
    def fetch_many(self, _ids: List, fields: Dict = None) -> Dict:
        """
        Read multiple records by _id, selecting only the designated fields (or
        all by default).
        """

    @abstractmethod
    def create(self, data: Dict) -> Dict:
        """
        Create a new record with the _id. If the _id is contained is not
        contained in the data dict nor provided as the _id argument, it is the
        responsibility of the Dao class to generate the _id.
        """

    @abstractmethod
    def create_many(self, records: List[Dict]) -> None:
        """
        Create a new record.  It is the responsibility of the Dao class to
        generate the _id.
        """

    @abstractmethod
    def update(self, _id, data: Dict) -> Dict:
        """
        Update a record with the data passed in.
        """

    @abstractmethod
    def update_many(self, _ids: List, data: List[Dict] = None) -> None:
        """
        Update multiple records. If a single data dict is passed in, then try to
        apply the same update to all records; otherwise, if a list of data dicts
        is passed in, try to zip the _ids with the data dicts and apply each
        unique update or each group of identical updates individually.
        """

    @abstractmethod
    def delete(self, _id) -> None:
        """
        Delete a single record.
        """

    @abstractmethod
    def delete_many(self, _ids: List) -> None:
        """
        Delete multiple records.
        """
