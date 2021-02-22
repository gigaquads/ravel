import bisect
import time

import numpy as np
import BTrees.OOBTree

from copy import deepcopy
from collections import defaultdict, Counter
from threading import RLock
from functools import reduce
from typing import Text, Dict, List, Set, Tuple, Type

from BTrees.OOBTree import BTree

from appyratus.utils.dict_utils import DictUtils

from ravel.schema import Schema, Field, fields
from ravel.constants import ID, REV
from ravel.util import union
from ravel.query.order_by import OrderBy
from ravel.query.predicate import (
    Predicate,
    ConditionalPredicate,
    BooleanPredicate,
    OP_CODE,
)

from .base import Store


class SimulationStore(Store):
    """
    An in-memory Store that stores data in Python dicts with BTrees indexes.
    """

    def __init__(self):
        super().__init__()
        self.reset()

    def on_bind(self, resource_type: Type['Resource'], **kwargs):
        for k, field in resource_type.ravel.schema.fields.items():
            if field.scalar and (type(field) is not Field):
                self.indexes[k] = BTree()

    def reset(self):
        """
        Reset all internal data structures.
        """
        self.lock = RLock()
        self.indexes = {}
        self.records = {}

    def exists(self, _id) -> bool:
        """
        Does the _id exist in the store?
        """
        with self.lock:
            return _id in self.records

    def exists_many(self, _ids: List) -> Dict[object, bool]:
        with self.lock:
            return {
                _id: (_id in self.records)
                for _id in _ids
            }

    def count(self) -> int:
        """
        Return the total number of objects in the store.
        """
        with self.lock:
            return len(self.records)

    def fetch(self, _id, fields: Set[Text] = None) -> Dict:
        """
        Return a single record.
        """
        return self.fetch_many([_id], fields=fields).get(_id)

    def fetch_many(self, _ids: List, fields=None) -> Dict:
        """
        Return multiple records in a _id-keyed dict.
        """
        if not fields:
            fields = set(self.resource_type.ravel.schema.fields)
        elif not isinstance(fields, set):
            fields = set(fields)

        with self.lock:
            records = {}

            for _id in _ids:
                # return a *copy* so as not to mutate dict in store
                record = deepcopy(self.records.get(_id))
                records[_id] = record

                # remove keys not specified in "fields" argument
                if fields and (record is not None):
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
        schema = self.resource_type.ravel.schema

        with self.lock:
            record[ID] = self.create_id(record)
            record[REV] = self.increment_rev()

            _id = record[ID]

            # insert the record and update indexes for its fields
            self.records[_id] = record
            self._index_upsert(_id, record)

        return deepcopy(record)

    def create_many(self, records: List[Dict] = None) -> List[Dict]:
        """
        Insert multiple records into the store, indexing their indexable
        fields.
        """
        results = []
        with self.lock:
            for record in records:
                record[ID] = self.create_id(record)
                result = self.create(record)
                results.append(result)
        return results

    def update(self, _id=None, data: Dict = None) -> Dict:
        """
        Submit changes to an object in the store.
        """
        with self.lock:
            old_record = self.records.get(_id, {})
            old_rev = old_record.get(REV)

            # remove the old copy of the object from the store, and then replace
            # it. we use `internal` in the call to delete to indicate that we
            # are only deleting for the purpose of re-inserting during this
            # update. this prevents the _rev index from being cleared.
            if old_record:
                self.delete(
                    old_record[ID],
                    index_names=set(data.keys()),
                    internal=True,
                )

            merged_record = DictUtils.merge(old_record, data)
            merged_record[REV] = self.increment_rev(old_rev)

            # "re-insert" the new copy of the record
            record = self.create(merged_record)
            return record

    def update_many(self, _ids: Set, data: Dict = None) -> Dict:
        """
        Submit changes to multiple objects in the store.
        """
        with self.lock:
            return {
                _id: self.update(_id=_id, data=x)
                for _id, x in zip(_ids, data)
            }

    def delete(self, _id, index_names: Set[Text] = None, internal=False):
        """
        Delete one record along with its field indexes.
        """
        with self.lock:
            record = self.records.get(_id)
            if record:
                index_names = set(index_names or record.keys())
                if internal:
                    index_names.discard(REV)
                self._index_remove(_id, record, index_names)
                del self.records[_id]

    def delete_many(self, _ids: List, internal=False):
        """
        Delete multiple records along with their field indexes.
        """
        with self.lock:
            for _id in _ids:
                self.delete(_id, internal=internal)

    def delete_all(self):
        """
        Delete all records along with their field indexes.
        """
        self.delete_many(self.records.keys())

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
        with self.lock:
            # compute set of ID's of records whose fields satisfy
            # the given "where" predicate of the query
            computed_ids = self._eval_predicate(predicate)
            records = list(self.fetch_many(computed_ids, fields).values())

            # order the records
            if order_by:
                records = OrderBy.sort(records, order_by)

            # paginate after ordering
            if offset is not None:
                if limit is not None:
                    records = records[offset:offset+limit]
                else:
                    records = records[offset:]
            elif limit is not None:
                records = records[:limit]

            return records

    def _index_upsert(self, _id, record):
        for k, v in record.items():
            index = self.indexes.get(k)
            if index is not None:
                if v not in index:
                    index[v] = set()
                index[v].add(_id)

    def _index_remove(self, _id, record, index_names=None):
        index_names = set(index_names or record.keys())
        for k in index_names:
            index = self.indexes.get(k)
            if index is not None and k in record:
                if k in record:
                    v = record[k]
                    if v in index:
                        index[v].remove(_id)

    def _eval_predicate(self, predicate):
        if predicate is None:
            return self.records.keys()

        op = predicate.op
        empty = set()
        computed_ids = set()

        if isinstance(predicate, ConditionalPredicate):
            k = predicate.field.source
            v = predicate.value
            index = self.indexes[k]

            if op == OP_CODE.EQ:
                computed_ids = index.get(v, empty)
            elif op == OP_CODE.NEQ:
                computed_ids = union([
                    id_set for v_idx, id_set in index.items()
                    if v_idx != v
                ])
            elif op == OP_CODE.INCLUDING:
                # containment - we compute the union of all sets of ids whose
                # corresponding records have the given values in the index
                v = v if isinstance(v, set) else set(v)
                computed_ids = union([index.get(k_idx, empty) for k_idx in v])

            elif op == OP_CODE.EXCLUDING:
                # the inverse of containment...
                v = v if isinstance(v, set) else set(v)
                computed_ids = union([
                    id_set for v_idx, id_set in index.items()
                    if v_idx not in v
                ])
            else:
                # handle inequalities, computing limit and offset to form an
                # interval with which we index the actual BTree

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
                    # XXX: raise StoreError
                    raise Exception('unrecognized op')

                computed_ids = union([
                    index[k] for k in keys[interval] if k is not None
                ])

        elif isinstance(predicate, BooleanPredicate):
        # recursively compute and union child predicates,
        # left-hand side (lhs) and right-hand side (rhs)
            lhs = predicate.lhs
            rhs = predicate.rhs

            if op == OP_CODE.AND:
                lhs_result = self._eval_predicate(lhs)
                if lhs_result:
                    rhs_result = self._eval_predicate(rhs)
                    computed_ids = set.intersection(lhs_result, rhs_result)

            elif op == OP_CODE.OR:
                lhs_result = self._eval_predicate(lhs)
                rhs_result = self._eval_predicate(rhs)
                computed_ids = set.union(lhs_result, rhs_result)

            else:
                # XXX: raise StoreError
                raise Exception(f'unrecognized predicate operator: {op}')

        return computed_ids
