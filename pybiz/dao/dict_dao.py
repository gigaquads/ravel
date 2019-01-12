import uuid
import bisect

import numpy as np
import BTrees.OOBTree

from copy import deepcopy
from collections import defaultdict, Counter
from threading import RLock
from functools import reduce
from typing import Text, Dict, List, Set, Tuple

from BTrees.OOBTree import BTree

from appyratus.utils import DictUtils

from pybiz.predicate import (
    Predicate,
    ConditionalPredicate,
    BooleanPredicate,
)

from .base import Dao
from .cache_dao import CacheInterface, CacheRecord


class DictDao(Dao, CacheInterface):
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

    def next_id(self):
        with self.lock:
            _id = self.id_counter
            self.id_counter += 1
            return _id

    def exists(self, _id) -> bool:
        with self.lock:
            return _id in self.records

    def fetch(self, _id, fields=None) -> Dict:
        with self.lock:
            record = deepcopy(self.records.get(_id))
            if record is not None:
                if fields is not None:
                    if fields:
                        for k in set(record.keys()) - set(fields):
                            del record[k]
                    else:
                        record = {'_id': _id}
            return record

    def fetch_many(self, _ids: List, fields=None) -> Dict:
        with self.lock:
            records = {}
            for _id in _ids:
                record = deepcopy(self.records.get(_id))
                if record is not None:
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
            self.rev_counter[_id] += 1
            for k, v in record.items():
                if not isinstance(v, dict):
                    if v not in self.indexes[k]:
                        self.indexes[k][v] = set()
                    self.indexes[k][v].add(_id)
        return deepcopy(record)

    def create_many(self, records: List = None) -> Dict:
        results = {}
        with self.lock:
            for record in records:
                _id = record['_id']
                result = self.create(record)
                results[_id] = result
            return results

    def update(self, _id=None, data: Dict = None) -> Dict:
        with self.lock:
            old_record = self.records.get(_id, {})
            old_rev = self.rev_counter.get(_id, 0)
            if old_record:
                self.delete(old_record['_id'])
            record = self.create(DictUtils.merge(old_record, data))
            self.rev_counter[_id] += old_rev
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
            self.records.pop(_id, None)
            self.rev_counter.pop(_id, None)
            for k, v in record.items():
                self.indexes[k][v].remove(_id)
            return record

    def delete_many(self, _ids: List) -> List:
        with self.lock:
            return {_id: self.delete(_id) for _id in _ids}

    def query(
        self,
        predicate: Predicate,
        fields: Set[Text] = None,
        order_by: Tuple[Text] = None,
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
                    raise Exception('unrecognized boolean predicate')

            return _ids

        with self.lock:
            _ids = process(predicate)
            results = list(self.fetch_many(_ids, fields=fields).values())
            if order_by:
                results = sorted(results, key=lambda x: tuple(
                    x[k] if k[0] != '-' else -1 * x[k][1:]
                    for k in order_by
                ))
            return results

    def fetch_cache(self, _ids: Set, rev=True, data=False, fields: Set = None) -> Dict:
        do_fetch_many = data   # alias to something more meaningful
        do_fetch_rev = rev     # "

        cache_records = defaultdict(CacheRecord)

        if do_fetch_many:
            records = self.fetch_many(_ids, fields=fields)
            for _id, record in records.items():
                cache_record = cache_records[_id]
                cache_record.data = record
                if do_fetch_rev:
                    cache_record.rev = self.rev_counter.setdefault(_id, 1)
        elif do_fetch_rev:
            for _id in _ids:
                cache_records[_id] = CacheRecord(
                    rev=self.rev_counter.get(_id)
                )

        return cache_records
