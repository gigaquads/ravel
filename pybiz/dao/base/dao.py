import uuid

import venusian

from threading import local
from collections import defaultdict
from typing import Dict, List, Type, Set, Text, Tuple
from abc import ABCMeta, abstractmethod

from appyratus.env import Environment

from .dao_history import DaoHistory, DaoEvent


class DaoMeta(ABCMeta):

    _local = local()
    _local.is_bootstrapped = defaultdict(bool)

    def __init__(cls, name, bases, dict_):
        ABCMeta.__init__(cls, name, bases, dict_)

        def callback(scanner, name, dao_type):
            scanner.dao_types.setdefault(name, dao_type)

        venusian.attach(cls, callback, category='dao')

        # wrap each DAO interface method in a decorator that appends
        # to the instance's DaoHistory when set.
        DaoHistory.decorate(dao_type=cls)


class Dao(object, metaclass=DaoMeta):

    env = Environment()

    def __init__(self, history=False, *args, **kwargs):
        self._history = DaoHistory(dao=self)
        self._is_bound = False
        self._biz_type = None
        self._registry = None

    def __repr__(self):
        if self.is_bound:
            return (
                f'<{self.__class__.__name__}'
                f'({self.biz_type.__name__})>'
            )
        else:
            return (f'<{self.__class__.__name__}>')

    @property
    def is_bound(self):
        return self._is_bound

    @property
    def biz_type(self):
        return self._biz_type

    @property
    def registry(self):
        return self._registry

    @property
    def history(self):
        return self._history

    def play(self, history: DaoHistory, reads=True, writes=True) -> List:
        results = []
        for event in history:
            is_read = event.method in DaoHistory.read_method_names
            is_write = event.method in DaoHistory.write_method_names
            if (is_read and reads) or (is_write and writes):
                func = getattr(self, event.method)
                result = func(*event.args, **event.kwargs)
                results.append(result)
        return results

    def bind(self, biz_type: Type['BizObject'], **kwargs):
        self._biz_type = biz_type
        self._is_bound = True
        self.on_bind(biz_type, **kwargs)

    @classmethod
    def bootstrap(cls, registry: 'Registry' = None, **kwargs):
        """
        Perform class-level initialization, like getting
        a connectio pool, for example.
        """
        cls._registry = registry
        cls.on_bootstrap(**kwargs)

        # TODO: put this into a method
        if not hasattr(DaoMeta._local, 'is_bootstrapped'):
            DaoMeta._local.is_bootstrapped = defaultdict(bool)

        DaoMeta._local.is_bootstrapped[cls.__name__] = True

    @classmethod
    def on_bootstrap(cls, **kwargs):
        pass

    def on_bind(cls, biz_type: Type['BizObject'], **kwargs):
        pass

    @classmethod
    def is_bootstrapped(cls):
        return cls._local.is_bootstrapped[cls.__name__]

    def create_id(self, record: Dict) -> object:
        """
        Generate and return a new ID for the given not-yet-created record.
        """
        return record.get('_id') or uuid.uuid4().hex

    @abstractmethod
    def exists(self, _id) -> bool:
        """
        Return True if the record with the given _id exists.
        """

    @abstractmethod
    def count(self) -> int:
        """
        Return the total number of stored records.
        """

    @abstractmethod
    def query(
        self,
        predicate: 'Predicate',
        fields: Set[Text] = None,
        limit: int = None,
        offset: int = None,
        order_by: Tuple = None,
        **kwargs
    ) -> List[Dict]:
        """
        Return all records whose fields match a logical predicate.
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
    def fetch_all(self, fields: Set[Text] = None) -> Dict:
        """
        Return all records managed by this Dao.
        """

    @abstractmethod
    def create(self, data: Dict) -> Dict:
        """
        Create a new record with the _id. If the _id is contained is not
        contained in the data dict nor provided as the _id argument, it is the
        responsibility of the Dao class to generate the _id.
        """

    @abstractmethod
    def create_many(self, records: List[Dict]) -> List[Dict]:
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
    def update_many(self, _ids: List, data: List[Dict] = None) -> List[Dict]:
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

    @abstractmethod
    def delete_all(self) -> None:
        """
        Delete all records.
        """
