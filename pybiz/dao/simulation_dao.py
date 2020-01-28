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
from pybiz.constants import ID_FIELD_NAME, REV_FIELD_NAME
from pybiz.predicate import (
    Predicate,
    ConditionalPredicate,
    BooleanPredicate,
    OP_CODE,
)

from .base import Dao


class SimulationDao(Dao):
    """
    An in-memory Dao that stores data in Python dicts with BTrees indexes.
    """

    def __init__(self):
        super().__init__()
        self.reset()

    @classmethod
    def on_bootstrap(cls):
        pass

    def on_bind(self, biz_class: Type['BizObject'], **kwargs):
        """
        This lifecycle method executes when Pybiz instantiates a singleton
        instance of this class and associates it with a specific BizObject
        class.
        """
        self.reset()
        # because we do no currently index composite data strucures, like dicts
        # and lists, we add the names of these fields on the bound BizObject
        # class to the list of "ignored" indexes.
        for k, v in biz_class.Schema.fields.items():
            if isinstance(v, (fields.Dict, fields.Nested, Schema)):
                self.ignored_indexes.add(k)

    def reset(self):
        """
        Reset all internal data structures.
        """
        self.lock = RLock()
        self.indexes = defaultdict(BTree)
        self.id_counter = 1
        self.rev_counter = Counter()
        self.records = {}
        self.ignored_indexes = set()

    def exists(self, _id) -> bool:
        """
        Does the _id exist in the store?
        """
        with self.lock:
            return _id in self.records

    def count(self) -> int:
        """
        Return the total number of objects in the store.
        """
        with self.lock:
            return len(self.records)

    def fetch(self, _id, fields=None) -> Dict:
        """
        Return a single record.
        """
        return self.fetch_many([_id], fields=fields)[_id]

    def fetch_many(self, _ids: List, fields=None) -> Dict:
        """
        Return multiple records in a _id-keyed dict.
        """
        with self.lock:
            records = {}
            fields = set(fields or [])

            for _id in _ids:
                # return a *copy* so as not to mutate the object in the store
                record = deepcopy(self.records.get(_id))
                records[_id] = record

                # remove and key not specified in "fields" arg
                if record is not None and fields:
                    record_keys = set(record.keys())
                    for k in record_keys - fields:
                        del record[k]

            return records

    def fetch_all(self, fields=None) -> Dict:
        """
        Return all records in a _id-keyed dict.
        """
        with self.lock:
            return deepcopy(self.records)

    def create(self, record: Dict = None) -> Dict:
        """
        Insert one record into the store, indexing its indexable fields.
        """
        schema = self.biz_class.pybiz.schema

        with self.lock:
            record[ID_FIELD_NAME] = _id = self.create_id(record)
            record[REV_FIELD_NAME] = self.rev_counter[_id]

            # ensure that missing nullable fields are written to the store with
            # a value of None.
            for k in (schema.nullable_fields.keys() - record.keys()):
                record[k] = None

            self.rev_counter[_id] += 1
            self.records[_id] = record

            self._update_indexes(_id, record)

        return deepcopy(record)

    def create_many(self, records: List[Dict] = None) -> List[Dict]:
        """
        Insert multiple records into the store, indexing their indexable
        fields.
        """
        results = []
        with self.lock:
            for record in records:
                record[ID_FIELD_NAME] = self.create_id(record)
                result = self.create(record)
                results.append(result)
        return results

    def update(self, _id=None, data: Dict = None) -> Dict:
        """
        Submit changes to an object in the store.
        """
        with self.lock:
            old_record = self.records.get(_id, {})
            old_rev = self.rev_counter.get(_id, 0)

            # remove the old copy of the object from the store,
            # and then replace it.
            if old_record:
                self.delete(
                    old_record[ID_FIELD_NAME],
                    clear_rev=False,
                    indexes_to_delete=set(data.keys()),
                )

            merged_record = DictUtils.merge(old_record, data)
            merged_record[REV_FIELD_NAME] = old_rev + 1

            # "re-insert" the new copy of the record
            record = self.create(merged_record)
            return record

    def update_many(self, _ids, data: Dict = None) -> Dict:
        """
        Submit changes to multiple objects in the store.
        """
        with self.lock:
            return [
                self.update(_id=_id, data=values)
                for _id, values in zip(_ids, data)
            ]

    def delete(self, _id, clear_rev=True, indexes_to_delete=None) -> Dict:
        """
        Delete one record along with its field indexes.
        """
        with self.lock:
            record = self.records.get(_id)
            self.records.pop(_id, None)
            if clear_rev:
                self.rev_counter.pop(_id, None)
            if record:
                self._delete_from_indexes(_id, record, indexes_to_delete)

            return record

    def delete_many(self, _ids: List, clear_rev=True) -> List:
        """
        Delete multiple records along with their field indexes.
        """
        _ids = list(_ids) if not isinstance(_ids, (list, tuple, set)) else _ids
        with self.lock:
            return {
                _id: self.delete(_id, clear_rev=clear_rev)
                for _id in _ids
            }

    def delete_all(self):
        """
        Delete all records along with their field indexes.
        """
        return self.delete_many(self.records.keys())

    def query(
        self,
        predicate: Predicate,
        fields: Set[Text] = None,
        order_by: Tuple = None,
        limit: int = None,
        offset: int = None,
        **kwargs
    ) -> List:
        """
        """
        records = []

        with self.lock:
            _ids = self._compute_queried_ids(predicate)
            records = list(self.fetch_many(_ids, fields=fields).values())

            # post processing, like ordering and pagination
            if order_by:
                for order_by_spec in order_by:
                    records = sorted(
                        records,
                        key=lambda x: x[order_by_spec.key],
                        reverse=order_by_spec.desc
                    )

            if offset is not None:
                if limit is not None:
                    records = records[offset:offset+limit]
                else:
                    records = records[offset:]
            elif limit is not None:
                records = records[:limit]

        return records

    def _update_indexes(self, _id, record):
        schema = self.biz_class.pybiz.schema
        record, error = schema.process(record, strict=True)
        for k, v in record.items():
            if k not in self.ignored_indexes:
                if v not in self.indexes[k]:
                    self.indexes[k][v] = set()
                self.indexes[k][v].add(_id)

    def _delete_from_indexes(self, _id, record, indexes_to_delete=None):
        schema = self.biz_class.pybiz.schema
        indexes_to_delete = indexes_to_delete or set(record.keys())
        record, error = schema.process(record, strict=True)
        for k in indexes_to_delete:
            if k not in self.ignored_indexes:
                v = record[k]
                self.indexes[k][v].remove(_id)

    def _union(self, sequences):
        if sequences:
            if len(sequences) == 1:
                return sequences[0]
            else:
                return set.union(*sequences)
        else:
            return set()

    def _compute_queried_ids(self, predicate):
        if predicate is None:
            return self.records.keys()

        op = predicate.op
        empty = set()
        _ids = set()

        if isinstance(predicate, ConditionalPredicate):
            k = predicate.field.source
            v = predicate.value
            index = self.indexes[k]

            if op == OP_CODE.EQ:
                _ids = self.indexes[k].get(v, empty)
            elif op == OP_CODE.NEQ:
                _ids = self._union([
                    _id_set for v_idx, _id_set in index.items()
                    if v_idx != v
                ])
            elif op == OP_CODE.INCLUDING:
                v = v if isinstance(v, set) else set(v)
                _ids = self._union([index.get(k_idx, empty) for k_idx in v])
            elif op == OP_CODE.EXCLUDING:
                v = v if isinstance(v, set) else set(v)
                _ids = self._union([
                    _id_set for v_idx, _id_set in index.items()
                    if v_idx not in v
                ])
            else:
                keys = np.array(index.keys(), dtype=object)
                if op == OP_CODE.GEQ:
                    offset = bisect.bisect_left(keys, v)
                    interval = slice(offset, None, 1)
                elif op == OP_CODE.GT:
                    offset = bisect.bisect(keys, v)
                    interval = slice(offset, None, 1)
                elif op == OP_CODE.LT:
                    offset = bisect.bisect_left(keys, v)
                    interval = slice(0, offset, 1)
                elif op == OP_CODE.LEQ:
                    offset = bisect.bisect(keys, v)
                    interval = slice(0, offset, 1)
                else:
                    # XXX: raise DaoError
                    raise Exception('unrecognized op')
                _ids = self._union([
                    index[k] for k in keys[interval]
                    if k is not None
                ])
        elif isinstance(predicate, BooleanPredicate):
            lhs = predicate.lhs
            rhs = predicate.rhs
            if op == OP_CODE.AND:
                lhs_result = self._compute_queried_ids(lhs)
                if lhs_result:
                    rhs_result = self._compute_queried_ids(rhs)
                    _ids = set.intersection(lhs_result, rhs_result)
            elif op == OP_CODE.OR:
                lhs_result = self._compute_queried_ids(lhs)
                rhs_result = self._compute_queried_ids(rhs)
                _ids = set.union(lhs_result, rhs_result)
            else:
                # XXX: raise DaoError
                raise Exception('unrecognized boolean predicate')

        return _ids
