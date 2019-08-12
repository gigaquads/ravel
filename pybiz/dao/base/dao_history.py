import traceback

from typing import Dict, List, Type, Text, Tuple
from collections import deque

from appyratus.utils import TimeUtils

from pybiz.util.loggers import console


class DaoEvent(object):
    def __init__(
        self,
        method: Text,
        args: Tuple = None,
        kwargs: Dict = None,
        exc: Exception = None,
    ):
        self.method = method
        self.args = args or tuple()
        self.kwargs = kwargs or {}
        self.timestamp = TimeUtils.utc_now()
        self.exc = exc

    def dump(self):
        return {
            'timestamp': self.timestamp,
            'method': self.method,
            'args': self.args,
            'kwargs': self.kwargs,
            'exc': self.exc,
        }

    def __repr__(self):
        return f'<DaoEvent({self.method}, {self.timestamp})>'


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
        self._events = deque()
        self._is_recording = False
        self._do_record_reads = reads
        self._do_record_writes = writes

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

    @property
    def is_recording(self):
        return self._is_recording

    @property
    def events(self):
        return self._events

    @classmethod
    def decorate(cls, dao_type: Type['Dao']):
        method_names = cls.read_method_names | cls.write_method_names
        for method_name in method_names:
            func = getattr(dao_type, method_name)
            decorator = cls._build_dao_interface_method_decorator(func)
            setattr(dao_type, method_name, decorator)

    @classmethod
    def _build_dao_interface_method_decorator(cls, func):
        def dao_interface_method_decorator(dao, *args, **kwargs):
            exc = None
            try:
                retval = func(dao, *args, **kwargs)
            except Exception as exc:
                retval = None
                console.error(
                    message=(
                        f'{dao} "{func.__name__}" failed'
                    ),
                    data={
                        'args': args,
                        'kwargs': kwargs,
                        'traceback': traceback.format_exc().split('\n')
                    }
                )

            # record the historical DAO event
            method_name = func.__name__
            if cls._do_record_event(dao.history, method_name):
                event = DaoEvent(method_name, args, kwargs, exc=exc)

                # XXX: hacky code below...
                # we need to insert newly created ID's into the unsaved
                # records passed into the create methods so that, when
                # replayed, the _id is taken from here rather than generated
                # anew by the replaying DAO inside its own create methods.
                if retval:
                    if event.method == 'create':
                        args[0]['_id'] = retval.get('_id')
                    elif event.method == 'create_many':
                        for record, created_record in zip(args[0], retval):
                            record['_id'] = created_record.get('_id')

                dao.history.append(event)
                if exc is not None:
                    raise exc

            return retval

        dao_interface_method_decorator.__name__ = (
            f'{func.__name__}_history_decorator'
        )
        return dao_interface_method_decorator

    @classmethod
    def _do_record_event(cls, history, name):
        if (
            (not history.is_recording) or
            (name in cls.read_method_names and not history._do_record_reads) or
            (name in cls.write_method_names and not history._do_record_writes)
        ):
            return False
        return True
