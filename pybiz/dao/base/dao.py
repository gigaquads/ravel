import os
import uuid
import inspect

import venusian

from threading import local
from collections import defaultdict
from typing import Dict, List, Type, Set, Text, Tuple
from abc import ABCMeta, abstractmethod
from pprint import pprint, pformat

from appyratus.utils import TimeUtils


class DaoEvent(object):
    def __init__(
        self,
        method: Text,
        args: Tuple = None,
        kwargs: Dict = None,
    ):
        self.method = method
        self.args = args or tuple()
        self.kwargs = kwargs or {}
        self.created_at = TimeUtils.utc_now()

    def __repr__(self):
        return f'<DaoEvent({self.method} @ {self.created_at})>'


class DaoHistory(object):

    read_method_names = frozenset({
        'fetch', 'fetch_all', 'fetch_many', 'count', 'exists', 'query'
    })
    write_method_names = frozenset({
        'create', 'create_many', 'update', 'update_many', 'delete',
        'delete_many', 'delete_all'
    })

    def __init__(self, dao: 'Dao', reads=True, writes=True):
        self._dao = dao
        self._events = []
        self.do_record_reads = reads
        self.do_record_writes = writes
        self._is_recording = False

    def __len__(self):
        return len(self._events)

    def __iter__(self):
        return iter(self._events)

    def __getitem__(self, idx):
        return self._events[idx]

    def start(self):
        self._is_recording = True

    def stop(self):
        self._is_recording = False

    def clear(self):
        self._events = []

    def append(self, event):
        self._events.append(event)

    def replay(self, dao: 'Dao', reads=True, writes=True) -> List:
        results = []
        for event in self._events:
            is_read = event.method in self.read_method_names
            is_write = event.method in self.write_method_names
            if (is_read and reads) or (is_write and writes):
                func = getattr(dao, event.method)
                result = func(*event.args, **event.kwargs)
                results.append(result)
        return results

    def report(self):
        print('DAO History')
        print('-' * 80)
        for e in self._events:
            func = getattr(self._dao, e.method)
            sig = inspect.signature(func)
            print(f'{e.created_at} - {e.method}')
            if e.args:
                print('Args:')
                for param, arg in zip(list(sig.parameters.keys())[:len(e.args)], e.args):
                    print(f'{param}: {pformat(arg)}')
                print()
            if e.kwargs:
                print('Kwargs:')
                for i, (k, v) in enumerate(kwargs.items()):
                    pprint(v)
                print()

    @property
    def is_recording(self):
        return self._is_recording

    @classmethod
    def decorate(cls, dao_type: Type['Dao']):
        method_names = cls.read_method_names | cls.write_method_names
        for method_name in method_names:
            method_func = getattr(dao_type, method_name)
            decorator = cls._build_dao_interface_method_decorator(method_func)
            setattr(dao_type, method_name, decorator)

    @classmethod
    def _build_dao_interface_method_decorator(cls, func):
        def dao_interface_method_decorator(self, *args, **kwargs):
            retval = func(self, *args, **kwargs)
            method_name = func.__name__

            # record the historical DAO event
            if cls._do_record_event(self.history, method_name):
                event = DaoEvent(method_name, args, kwargs)

                # we need to insert newly created ID's into the unsaved
                # records passed into the create methods so that, when
                # replayed, the _id is taken from here rather than generated
                # anew by the replaying DAO inside its own create methods.
                if event.method == 'create':
                    args[0]['_id'] = retval['_id']
                elif event.method == 'create_many':
                    for record, created_record in zip(args[0], retval):
                        record['_id'] = created_record['_id']

                self.history.append(event)

            return retval

        dao_interface_method_decorator.__name__ = (
            f'{func.__name__}_history_decorator'
        )
        return dao_interface_method_decorator

    @classmethod
    def _do_record_event(cls, history, name):
        if (
            (not history.is_recording) or
            (name in cls.read_method_names and not history.do_record_reads) or
            (name in cls.write_method_names and not history.do_record_writes)
        ):
            return False
        return True


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
    def __init__(self, history=False, *args, **kwargs):
        self._is_bound = False
        self._biz_type = None
        self._registry = None

        self.history = DaoHistory(dao=self)
        self.ignore_rev = False  # XXX: hacky, for CacheDao to work

    def __repr__(self):
        if self.is_bound:
            return (
                f'<{self.__class__.__name__}'
                f'({self.biz_type.__name__})>'
            )
        else:
            return (
                f'<{self.__class__.__name__}>'
            )

    @property
    def is_bound(self):
        return self._is_bound

    @property
    def biz_type(self):
        return self._biz_type

    @property
    def registry(self):
        return self._registry

    def bind(self, biz_type: Type['BizObject']):
        self._biz_type = biz_type
        self._is_bound = True

    @classmethod
    def bootstrap(cls, registry: 'Registry' = None, **kwargs):
        """
        Perform class-level initialization, like getting
        a connectio pool, for example.
        """
        cls._registry = registry
        cls.on_bootstrap()

        # TODO: put this into a method
        DaoMeta._local.is_bootstrapped[cls.__name__] = True

    @classmethod
    def on_bootstrap(cls, **kwargs):
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
