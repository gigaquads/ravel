import venusian

from threading import local
from collections import defaultdict
from typing import Dict, List, Type, Set, Text, Tuple
from abc import ABCMeta, abstractmethod

from appyratus.env import Environment
from appyratus.schema.fields import UuidString

from pybiz.util.loggers import console
from pybiz.exceptions import PybizError
from pybiz.constants import ID_FIELD_NAME

from .store_history import StoreHistory, StoreEvent


class StoreError(PybizError):
    pass


class StoreMeta(ABCMeta):

    _local = local()
    _local.is_bootstrapped = defaultdict(bool)

    def __init__(cls, name, bases, dict_):
        ABCMeta.__init__(cls, name, bases, dict_)

        def callback(scanner, name, store_class):
            scanner.store_classes.setdefault(name, store_class)
            console.info(f'venusian scan found "{store_class.__name__}" Store')

        venusian.attach(cls, callback, category='store')


class Store(object, metaclass=StoreMeta):

    env = Environment()
    _app = None

    def __init__(self, *args, **kwargs):
        self._history = StoreHistory(store=self)
        self._is_bound = False
        self._biz_class = None

    def __repr__(self):
        if self.is_bound:
            return (
                f'<{self.__class__.__name__}'
                f'(biz_class={self.biz_class.__name__})>'
            )
        else:
            return (f'<{self.__class__.__name__}>')

    def dispatch(
        self,
        method_name: Text,
        args: Tuple = None,
        kwargs: Dict = None
    ):
        """
        Delegate a Store call to the named method, performing any side-effects,
        like creating a StoreHistory event if need be. This is used internally
        by Resource to call into DAO methods.
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
            if not isinstance(exc, PybizError):
                raise PybizError(data=data, wrapped_exception=exc)
            else:
                exc.data.update(data)
                raise exc

        return result

    @property
    def is_bound(self) -> bool:
        return self._is_bound

    @property
    def biz_class(self) -> Type['Resource']:
        return self._biz_class

    @property
    def app(self) -> 'Application':
        return self._app

    @property
    def history(self) -> 'StoreHistory':
        return self._history

    def play(self, history: StoreHistory, reads=True, writes=True) -> List:
        results = []
        for event in history:
            is_read = event.method in self.history.read_method_names
            is_write = event.method in self.history.write_method_names
            if (is_read and reads) or (is_write and writes):
                func = getattr(self, event.method)
                result = func(*event.args, **event.kwargs)
                results.append(result)
        return results

    def bind(self, biz_class: Type['Resource'], **kwargs):
        self._biz_class = biz_class
        self.on_bind(biz_class, **kwargs)
        self._is_bound = True

    @classmethod
    def bootstrap(cls, app: 'Application' = None, **kwargs):
        """
        Perform class-level initialization, like getting
        a connectio pool, for example.
        """
        cls._app = app
        cls.on_bootstrap(**kwargs)

        # TODO: put this into a method
        if not hasattr(StoreMeta._local, 'is_bootstrapped'):
            StoreMeta._local.is_bootstrapped = defaultdict(bool)

        StoreMeta._local.is_bootstrapped[cls.__name__] = True

    @classmethod
    def on_bootstrap(cls, **kwargs):
        pass

    def on_bind(cls, biz_class: Type['Resource'], **kwargs):
        pass

    @classmethod
    def is_bootstrapped(cls):
        return cls._local.is_bootstrapped[cls.__name__]

    def create_id(self, record: Dict) -> object:
        """
        Generate and return a new ID for the given not-yet-created record.
        """
        new_id = record.get(ID_FIELD_NAME)
        if new_id is None:
            new_id = self.biz_class.pybiz.defaults[ID_FIELD_NAME]()

        # NOTE: if new_id is still None at this point, it's assumed that
        # the persistence technology will generate and return it instead.

        return new_id

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
