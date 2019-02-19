import uuid
import bisect

import numpy as np
import BTrees.OOBTree

from copy import deepcopy
from collections import defaultdict, Counter
from threading import RLock
from functools import reduce
from typing import Text, Dict, List, Set, Tuple, Type

from BTrees.OOBTree import BTree

from appyratus.utils import DictUtils

from pybiz.schema import Schema, fields
from pybiz.predicate import (
    Predicate,
    ConditionalPredicate,
    BooleanPredicate,
    OP_CODE,
)

from .base import Dao


class PythonDao(Dao):
    """
    An in-memory Dao that stores data in Python dicts with BTrees indexes.
    """

    def __init__(self):
        super().__init__()
        self.lock = RLock()
        self.indexes = defaultdict(BTree)
        self.id_counter = 1
        self.rev_counter = Counter()
        self.records = {}
        self.ignored_indexes = set()

    def on_bind(self, biz_type: Type['BizObject'], **kwargs):
        for k, v in biz_type.schema.fields.items():
            if isinstance(v, (fields.Dict, fields.Nested, Schema)):
                self.ignored_indexes.add(k)

    def exists(self, _id) -> bool:
        with self.lock:
            return _id in self.records

    def count(self) -> int:
        with self.lock:
            return len(self.records)

    def fetch(self, _id, fields=None) -> Dict:
        return self.fetch_many([_id], fields=fields)[_id]

    def fetch_many(self, _ids: List, fields=None) -> Dict:
        with self.lock:
            records = {}
            fields = set(fields or [])
            for _id in _ids:
                record = deepcopy(self.records.get(_id))
                records[_id] = record
                if record is not None:
                    if fields:
                        record_keys = set(record.keys())
                        missing_keys = fields - record_keys
                        for k in record_keys - fields:
                            del record[k]
                        if missing_keys:
                            record.update({k: None for k in missing_keys})
            return records

    def fetch_all(self, fields=None):
        with self.lock:
            return deepcopy(self.records)

    def create(self, record: Dict = None) -> Dict:
        with self.lock:
            record['_id'] = _id = self.create_id(record)
            record['_rev'] = self.rev_counter[_id]
            self.rev_counter[_id] += 1
            self.records[_id] = record
            self._update_indexes(_id, record)

        return deepcopy(record)

    def create_many(self, records: List[Dict] = None) -> List[Dict]:
        results = []
        with self.lock:
            for record in records:
                record['_id'] = self.create_id(record)
                result = self.create(record)
                results.append(result)
        return results

    def update(self, _id=None, data: Dict = None) -> Dict:
        with self.lock:
            old_record = self.records.get(_id, {})
            old_rev = self.rev_counter.get(_id, 0)

            if old_record:
                # TODO: only delete from updated indexes
                self.delete(old_record['_id'], clear_rev=False)

            merged_record = DictUtils.merge(old_record, data)
            merged_record['_rev'] = old_rev + 1

            record = self.create(merged_record)
            return record

    def update_many(self, _ids, data: Dict = None) -> Dict:
        with self.lock:
            return [
                self.update(_id=_id, data=values)
                for _id, values in zip(_ids, data)
            ]

    def delete(self, _id, clear_rev=True) -> Dict:
        with self.lock:
            record = self.records.get(_id)
            self.records.pop(_id, None)
            if clear_rev:
                self.rev_counter.pop(_id, None)
            if record:
                self._delete_from_indexes(_id, record)

            return record

    def delete_many(self, _ids: List, clear_rev=True) -> List:
        _ids = list(_ids) if not isinstance(_ids, (list, tuple, set)) else _ids
        with self.lock:
            return {
                _id: self.delete(_id, clear_rev=clear_rev)
                for _id in _ids
            }

    def delete_all(self):
        return self.delete_many(self.records.keys())

    def query(
        self,
        predicate: Predicate,
        fields: Set[Text] = None,
        order_by: Tuple = None,
        **kwargs
    ) -> List:
        def union(sequences):
            if sequences:
                if len(sequences) == 1:
                    return sequences[0]
                else:
                    return set.union(*sequences)
            else:
                return set()

        def process(predicate):
            if predicate is None:
                return self.records.keys()

            op = predicate.op
            empty = set()
            _ids = set()

            if isinstance(predicate, ConditionalPredicate):
                k = predicate.field.source
                v = predicate.value
                index = self.indexes[k]

                if op == '=':
                    _ids = self.indexes[k].get(v, empty)
                elif op == '!=':
                    _ids = union([
                        _id_set for v_idx, _id_set in index.items()
                        if v_idx != v
                    ])
                elif op == 'in':
                    v = v if isinstance(v, set) else set(v)
                    _ids = union([index.get(k_idx, empty) for k_idx in v])
                elif op == 'nin':
                    v = v if isinstance(v, set) else set(v)
                    _ids = union([
                        _id_set for v_idx, _id_set in index.items()
                        if v_idx not in v
                    ])
                else:
                    keys = np.array(index.keys(), dtype=object)
                    if op == '>=':
                        offset = bisect.bisect_left(keys, v)
                        interval = slice(offset, None, 1)
                    elif op == '>':
                        offset = bisect.bisect(keys, v)
                        interval = slice(offset, None, 1)
                    elif op == '<':
                        offset = bisect.bisect_left(keys, v)
                        interval = slice(0, offset, 1)
                    elif op == '<=':
                        offset = bisect.bisect(keys, v)
                        interval = slice(0, offset, 1)
                    else:
                        # XXX: raise DaoError
                        raise Exception('unrecognized op')
                    _ids = union([
                        index[k] for k in keys[interval]
                        if k is not None
                    ])
            elif isinstance(predicate, BooleanPredicate):
                lhs = predicate.lhs
                rhs = predicate.rhs
                if op == '&':
                    lhs_result = process(lhs)
                    if lhs_result:
                        rhs_result = process(rhs)
                        _ids = set.intersection(lhs_result, rhs_result)
                elif op == '|':
                    lhs_result = process(lhs)
                    rhs_result = process(rhs)
                    _ids = set.union(lhs_result, rhs_result)
                else:
                    # XXX: raise DaoError
                    raise Exception('unrecognized boolean predicate')

            return _ids

        results  = []

        with self.lock:
            _ids = process(predicate)
            results = list(self.fetch_many(_ids, fields=fields).values())
            if order_by:
                for item in order_by:
                    results = sorted(
                        results,
                        key=lambda x: x[item.key],
                        reverse=item.desc
                    )

        return results

    def _update_indexes(self, _id, record):
        for k, v in record.items():
            if k not in self.ignored_indexes:
                if v not in self.indexes[k]:
                    self.indexes[k][v] = set()
                self.indexes[k][v].add(_id)

    def _delete_from_indexes(self, _id, record):
        for k, v in record.items():
            if k not in self.ignored_indexes:
                self.indexes[k][v].remove(_id)
