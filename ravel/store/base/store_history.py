import traceback

from typing import Dict, List, Type, Text, Tuple
from collections import deque

from appyratus.utils.time_utils import TimeUtils

from ravel.constants import ID

READ_METHODS = frozenset({
    'fetch', 'fetch_all', 'fetch_many', 'count', 'exists', 'query'
})

WRITE_METHODS = frozenset({
    'create', 'create_many', 'update', 'update_many', 'delete',
    'delete_many', 'delete_all'
})


class StoreEvent(object):
    def __init__(
        self,
        method: Text,
        args: Tuple = None,
        kwargs: Dict = None,
        result = None,
        exc: Exception = None,
    ):
        self.method = method
        self.args = list(args) if args else []
        self.kwargs = kwargs or {}
        self.result = result
        self.timestamp = TimeUtils.utc_now()
        self.exc = exc

        # fix up create and create_many args and kwargs.
        # see the doc string for _backfill_id_fields.
        if (exc is None) and (method in {'create', 'create_many'}):
            self._backfill_id_fields(result)

    def __repr__(self):
        return (
            f'StoreEvent(method={self.method}, timestamp={self.timestamp})'
        )

    def _backfill_id_fields(self, result):
        """
        If we ever replay this history, we don't want to inadvertantly trigger
        the generation of new resource ID's in the store we're replaying it in.
        To prevent this, we inject the generated ID's from the return values of
        these methods into the args and kwargs that were originally passed in.
        """
        if self.method == 'create':
            self.args[0][ID] = result.get(ID)
        elif self.method == 'create_many':
            uncreated_records = args[0]
            created_records = result
            for uncreated, created in zip(uncreated_records, created_records):
                created_id = created_record.get(ID)
                uncreated_record[ID] = created_id

    def dump(self, with_result=False) -> Dict:
        data = {
            'timestamp': self.timestamp,
            'method': self.method,
            'args': self.args,
            'kwargs': self.kwargs,
            'exc': self.exc,
        }
        if with_result:
            data['result'] = self.result
        return data


class StoreHistory(object):
    def __init__(self, store: 'Store', reads=True, writes=True):
        self._store = store
        self._events = deque()
        self._is_recording = False
        self._is_recording_reads = reads
        self._is_recording_writes = writes

    def __len__(self):
        return len(self._events)

    def __iter__(self):
        return iter(self._events)

    def __getitem__(self, idx):
        return self._events[idx]

    def is_recording_method(self, name):
        return (
            self._is_recording and (
                (name in READ_METHODS and self._is_recording_reads) or
                (name in WRITE_METHODS and self._is_recording_writes)
            )
        )

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
