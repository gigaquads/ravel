import uuid
import bisect

import numpy as np
import BTrees.OOBTree

from copy import deepcopy
from collections import defaultdict
from threading import RLock
from functools import reduce
from typing import Text, Dict, List

from BTrees.OOBTree import BTree, TreeSet
from appyratus.utils import DictUtils

from pybiz.predicate import (
    Predicate,
    ConditionalPredicate,
    BooleanPredicate,
)

from .base import Dao


class DictDao(Dao):
    """
    """

    _lock = RLock()
    _indexes = defaultdict(BTree)
    _records = {}

    @classmethod
    def next_id(cls):
        return uuid.uuid4().hex

    def exists(self, _id) -> bool:
        with self._lock:
            return _id in self._indexes['_id']

    def fetch(self, _id, fields=None) -> Dict:
        with self._lock:
            return deepcopy(self._records.get('_id'))

    def fetch_many(self, _ids: List, fields=None) -> Dict:
        with self._lock:
            return {
                _id: deepcopy(self._records.get('_id'))
                for _id in _ids
            }

    def create(self, record: Dict = None) -> Dict:
        with self._lock:
            _id = self.next_id()
            record['_id'] = _id
            self._records[_id] = record
            for k, v in record.items():
                if not isinstance(v, dict):
                    if v not in self._indexes[k]:
                        self._indexes[k][v] = TreeSet()
                    self._indexes[k][v].add(_id)
        return record

    def create_many(self, records: List = None) -> Dict:
        results = {}
        with self._lock:
            for record in records:
                result = self.create(record)
                results[result['_id']] = result
            return results

    def update(self, _id=None, data: Dict = None) -> Dict:
        with self._lock:
            record = self._data['_id'].get(_id, {})
            DictUtils.merge(record, data)
            self._records[_id] = record
            return record

    def update_many(self, _ids: List, data: List = None) -> Dict:
        with self._lock:
            return {
                _id: self.update(_id=_id, data=data_dict)
                for _id, data_dict in zip(_ids, data)
            }

    def delete(self, _id) -> Dict:
        with self._lock:
            return self._indexes['_id'].pop(_id, None)

    def delete_many(self, _ids: List) -> List:
        with self._lock:
            return {_id: self.delete(_id) for _id in _ids}

    def query(self, predicate: Predicate, **kwargs) -> List:
        def union(sequences):
            if sequences:
                if len(sequences) == 1:
                    return sequences[0]
                else:
                    return reduce(BTrees.OOBTree.union, sequences)
            else:
                return BTrees.OOBTree.TreeSet()

        def process(predicate):
            op = predicate.op
            k = predicate.attr_name
            v = predicate.value
            empty = TreeSet()
            index = self._indexes[k]
            _ids = set()

            if isinstance(predicate, ConditionalPredicate):
                if op == '=':
                    _ids = self._indexes[k].get(v, empty)
                elif op == '!=':
                    v = v if isinstance(v, set) else set(v)
                    _ids = union([
                        _id_set for v_idx, _id_set in index.items()
                        if v_idx != v
                    ])
                elif op == 'in':
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
            elif isinstance(pred, BooleanPredicate):
                if op == '&':
                    lhs_result = process(pred.lhs)
                    if lhs_result:
                        rhs_result = process(pred.rhs)
                        intersect = BTrees.OOBTree.intersection
                        _ids = intersect(lhs_result, rhs_result)
                elif pred.op == '|':
                    lhs_result = process(pred.lhs)
                    rhs_result = process(pred.rhs)
                    _ids = BTrees.OOBTree.union(lhs_result, rhs_result)
                else:
                    raise Exception('unrecognized boolean predicate')

            return _ids

        with self._lock:
            _ids = process(predicate)
            results = [self._records[_id] for _id in _ids]
            return results
