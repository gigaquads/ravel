from typing import Dict, List, Type, Text, Tuple

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
