import uuid
import bisect

import numpy as np
import BTrees.OOBTree

from copy import deepcopy
from collections import defaultdict
from threading import RLock
from functools import reduce
from typing import Text, Dict, List

from BTrees.OOBTree import BTree
from appyratus.utils import DictUtils

from pybiz.predicate import (
    Predicate,
    ConditionalPredicate,
    BooleanPredicate,
)

from .base import Dao


class DictDao(Dao):
    """
    An in-memory Dao that stores data in Python dicts with BTrees indexes.
    """

    _locks = defaultdict(RLock)
    _indexes = defaultdict(lambda: defaultdict(BTree))
    _records = defaultdict(dict)
    _next_id = defaultdict(lambda: 1)

    def next_id(self):
        with self._locks[self.type_name]:
            _id = self._next_id[self.type_name]
            self._next_id[self.type_name] += 1
            return _id

    def __init__(self, type_name: Text):
        super().__init__()
        self.type_name = type_name
        self.lock = self._locks[type_name]
        self.indexes = self._indexes[type_name]
        self.records = self._records[type_name]

    def exists(self, _id) -> bool:
        with self.lock:
            return _id in self.indexes['_id']

    def fetch(self, _id, fields=None) -> Dict:
        with self.lock:
            record = deepcopy(self.records.get(_id))
            if fields:
                for k in set(record.keys()) - set(fields):
                    del record[k]
            return record

    def fetch_many(self, _ids: List, fields=None) -> Dict:
        with self.lock:
            records = {}
            for _id in _ids:
                record = deepcopy(self.records.get(_id))
                records[_id] = record
                if fields:
                    for k in set(record.keys()) - set(fields):
                        del record[k]
            return records

    def create(self, record: Dict = None) -> Dict:
        with self.lock:
            _id = record.get('_id') or self.next_id()
            record['_id'] = _id
            self.records[_id] = record
            for k, v in record.items():
                if not isinstance(v, dict):
                    if v not in self.indexes[k]:
                        self.indexes[k][v] = set()
                    self.indexes[k][v].add(_id)
        return record

    def create_many(self, records: List = None) -> Dict:
        results = {}
        with self.lock:
            for record in records:
                result = self.create(record)
                results[result['_id']] = result
            return results

    def update(self, _id=None, data: Dict = None) -> Dict:
        with self.lock:
            old_record = self.records.get(_id, {})
            if old_record:
                self.delete(old_record['_id'])
            record = self.create(DictUtils.merge(old_record, data))
            return record

    def update_many(self, _ids: List, data: List = None) -> Dict:
        with self.lock:
            return {
                _id: self.update(_id=_id, data=data_dict)
                for _id, data_dict in zip(_ids, data)
            }

    def delete(self, _id) -> Dict:
        with self.lock:
            record = self.records.get(_id)
            self.records.pop(_id, {})
            for k, v in record.items():
                self.indexes[k][v].remove(_id)
            return record

    def delete_many(self, _ids: List) -> List:
        with self.lock:
            return {_id: self.delete(_id) for _id in _ids}

    def query(self, predicate: Predicate, fields=None, **kwargs) -> List:
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
                k = predicate.attr_name
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
                        intersect = BTrees.OOBTree.intersection
                        _ids = intersect(lhs_result, rhs_result)
                elif op == '|':
                    lhs_result = process(lhs)
                    rhs_result = process(rhs)
                    _ids = BTrees.OOBTree.union(lhs_result, rhs_result)
                else:
                    raise Exception('unrecognized boolean predicate')

            return _ids

        with self.lock:
            _ids = process(predicate)
            results = list(self.fetch_many(_ids, fields=fields).values())
            return results
