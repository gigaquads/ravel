import time

import base36

from datetime import datetime
from collections import defaultdict
from typing import Dict, List, Type, Set, Text, Tuple
from threading import local
from abc import ABCMeta, abstractmethod

from appyratus.env import Environment
from appyratus.schema.fields import UuidString
from appyratus.utils.dict_utils import DictObject
from appyratus.utils.type_utils import TypeUtils

from ravel.util.loggers import console
from ravel.util.misc_functions import get_class_name
from ravel.exceptions import RavelError
from ravel.constants import ID

from .store_history import StoreHistory, StoreEvent


class StoreError(RavelError):
    pass


class StoreMeta(ABCMeta):
    def __init__(cls, name, bases, dict_):
        ABCMeta.__init__(cls, name, bases, dict_)
        cls.ravel = DictObject()
        cls.ravel.local = local()
        cls.ravel.local.is_bootstrapped = False


class Store(object, metaclass=StoreMeta):

    env = Environment()
    ravel = None

    def __init__(self, *args, **kwargs):
        self._history = StoreHistory(store=self)
        self._is_bound = False
        self._resource_type = None

    def __repr__(self):
        if self.is_bound:
            return (
                f'<{get_class_name(self)}'
                f'(resource_type={get_class_name(self.resource_type)})>'
            )
        else:
            return (f'<{get_class_name(self)}>')

    def dispatch(
        self,
        method_name: Text,
        args: Tuple = None,
        kwargs: Dict = None
    ):
        """
        Delegate a Store call to the named method, performing any side-effects,
        like creating a StoreHistory event if need be. This is used internally
        by Resource to call into store methods.
        """
        # call the requested Store method
        func = getattr(self, method_name)
        exc = None
        try:
            result = func(*(args or tuple()), **(kwargs or {}))
        except Exception as exc:
            raise exc

        # create and store the Store call in a history event
        if self.history.is_recording_method(method_name):
            event = StoreEvent(method_name, args, kwargs, result, exc)
            self._history.append(event)

        # finally faise the exception if one was generated
        if exc is not None:
            data = {'method': method_name, 'args': args, 'kwargs': kwargs}
            if not isinstance(exc, RavelError):
                raise RavelError(data=data, wrapped_exception=exc)
            else:
                exc.data.update(data)
                raise exc

        return result

    @property
    def is_bound(self) -> bool:
        return self._is_bound

    @property
    def resource_type(self) -> Type['Resource']:
        return self._resource_type

    @property
    def schema(self) -> 'Schema':
        if self._resource_type is not None:
            return self._resource_type.ravel.schema
        return None

    @property
    def app(self) -> 'Application':
        return self.ravel.app

    @property
    def history(self) -> 'StoreHistory':
        return self._history

    def replay(
        self,
        history: StoreHistory = None,
        reads=True,
        writes=True
    ) -> Dict[StoreEvent, object]:
        """
        Replay events (interface calls) from this or another store's history,
        returning an ordered mapping from the event object to the return value
        of the corresponding store method.

        Args:
        - `history`: the history to replay in this store.
        - `reads`: replay "read" events, like query, get, get_many.
        - `writes`: replay "write" events, like create, update, etc.
        """
        results = OrderedDict()

        # use this store's own history if none specified
        if history is None:
            history = self.history

        for event in history:
            is_read = event.method in self.history.read_method_names
            is_write = event.method in self.history.write_method_names
            if (is_read and reads) or (is_write and writes):
                func = getattr(self, event.method)
                result = func(*event.args, **event.kwargs)
                results[event] = result

        return results

    def bind(self, resource_type: Type['Resource'], **kwargs):
        t1 = datetime.now()
        self._resource_type = resource_type
        self.on_bind(resource_type, **kwargs)

        t2 = datetime.now()
        secs = (t2 - t1).total_seconds()
        console.debug(
            f'bound {TypeUtils.get_class_name(resource_type)} to '
            f'{TypeUtils.get_class_name(self)} '
            f'in {secs:.2f}s'
        )

        self._is_bound = True

    @classmethod
    def bootstrap(cls, app: 'Application' = None, **kwargs):
        """
        Perform class-level initialization, like getting
        a connectio pool, for example.
        """
        t1 = datetime.now()

        cls.ravel.app = app
        cls.on_bootstrap(**kwargs)

        cls.ravel.local.is_bootstrapped = True

        t2 = datetime.now()
        secs = (t2 - t1).total_seconds()
        console.debug(
            f'bootstrapped {TypeUtils.get_class_name(cls)} '
            f'in {secs:.2f}s'
        )

        return cls

    @classmethod
    def on_bootstrap(cls, **kwargs):
        pass

    def on_bind(cls, resource_type: Type['Resource'], **kwargs):
        pass

    @classmethod
    def is_bootstrapped(cls):
        return getattr(cls.ravel.local, 'is_bootstrapped', False)

    @classmethod
    def has_transaction(cls):
        raise NotImplementedError()

    @classmethod
    def begin(cls, **kwargs):
        raise NotImplementedError()

    @classmethod
    def commit(cls, **kwargs):
        raise NotImplementedError()

    @classmethod
    def rollback(cls, **kwargs):
        raise NotImplementedError()

    def create_id(self, record: Dict) -> object:
        """
        Generate and return a new ID for the given not-yet-created record. If
        this method returns None, the backend database/storage technology
        must create and return it instead.
        """
        new_id = record.get(ID)
        if new_id is None:
            new_id = self.resource_type.ravel.defaults[ID]()
        return new_id

    def increment_rev(self, rev: Text = None) -> Text:
        time_ms = int(1000000 * time.time())

        if rev:
            rev_no = int(rev.split('-')[1], 36) + 1
        else:
            rev_no = 1

        return f'{base36.dumps(time_ms)}-{base36.dumps(rev_no)}'


    @abstractmethod
    def exists(self, _id) -> bool:
        """
        Return True if the record with the given _id exists.
        """

    @abstractmethod
    def exists_many(self, _ids: Set) -> Dict[object, bool]:
        """
        Return a mapping from _id to a boolean, indicating if the specified
        resource exists.
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
        **kwargs
    ) -> List[Dict]:
        """
        TODO: rename to "select"
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
        Return all records managed by this Store.
        """

    @abstractmethod
    def create(self, data: Dict) -> Dict:
        """
        Create a new record with the _id. If the _id is contained is not
        contained in the data dict nor provided as the _id argument, it is the
        responsibility of the Store class to generate the _id.
        """

    @abstractmethod
    def create_many(self, records: List[Dict]) -> List[Dict]:
        """
        Create a new record.  It is the responsibility of the Store class to
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
