import threading
import bisect
import inspect

import numpy as np
import persistent
import BTrees.OOBTree
import BTrees.IOBTree
import ZODB.FileStorage
import ZODB

from typing import Dict, List, Text, Set
from functools import reduce

from persistent.mapping import PersistentMapping
from appyratus.validation import Schema, fields

from pybiz.predicate import ConditionalPredicate, BooleanPredicate

from .base import Dao


class ZodbObject(persistent.Persistent):
    def __init__(self, record: Dict, schema: Schema = None):
        super().__init__()
        setattr(self, '_id', record['_id'])
        for field in schema.fields.values():
            key = field.load_key
            if key in record:
                setattr(self, key, record[key])

    def __repr__(self):
        return '<{}>'.format(self.__class__.__name__)

    def __eq__(self, other):
        return self._id == other._id

    def __lt__(self, other):
        return self._id < other._id

    def __le__(self, other):
        return self._id <= other._id

    def __ge__(self, other):
        return self._id >= other._id

    def __gt__(self, other):
        return self._id > other._id

    def __hash__(self):
        return self._id


class ZodbCollection(object):
    def __init__(self, name: str, schema: Schema, specs=None):
        self.name = name
        self.schema = schema
        self.specs = {s.field_name: s for s in (specs or [])}
        self.root = None  # set in initialize
        self.data = None  # set in initialize

    def initialize(self, root: PersistentMapping, clear=False):
        self.root = root
        self.data = self.root.get(self.name)
        self.specs['_id'] = ZodbDao.IndexSpec('_id', unique=True)
        if (not self.data) or clear:
            self.root[self.name] = PersistentMapping()
            self.data = self.root[self.name]
            self.data['population'] = BTrees.OOBTree.BTree()
            self.data['indexes'] = PersistentMapping()
            unindexable_field_types = (
                fields.List, fields.Dict, fields.Object
            )
            self.data['indexes']['_id'] = BTrees.OOBTree.BTree()
            for field in self.schema.fields.values():
                if not isinstance(field, unindexable_field_types):
                    idx_name = field.load_key
                    idx = BTrees.OOBTree.BTree()
                    self.data['indexes'][idx_name] = idx

        # now create and new indexes due to
        # possible addition of new schema fields
        for field in self.schema.fields.values():
            if field.load_key not in self.data['indexes']:
                idx = BTrees.OOBTree.BTree()
                self.data['indexes'][field.load_key] = idx

    def insert(self, obj):
        if self.population.has_key(obj._id):
            raise Exception('unique constraint')
        self.population[obj._id] = obj
        self._insert_to_indexes(obj)

    def update(self, obj, updates: dict = None):
        if not self.population.has_key(obj._id):
            raise Exception('does not exist')
        self._remove_from_indexes(obj)
        for k, v in updates.items():
            setattr(obj, k, v)
        self._insert_to_indexes(obj)

    def remove(self, obj):
        if self.population.has_key(obj._id):
            del self.population[obj._id]
            self._remove_from_indexes(obj)

    def _insert_to_indexes(self, obj):
        for idx_name, btree in self.indexes.items():
            key = getattr(obj, idx_name, None)
            spec = self.specs.get(idx_name)
            tree_set = btree.get(key)
            if spec and spec.unique and tree_set:
                raise Exception('unique constraint')
            if tree_set is None:
                tree_set = BTrees.OOBTree.TreeSet()
                btree[key] = tree_set
            tree_set.insert(obj)

    def _remove_from_indexes(self, obj):
        for idx_name, btree in self.indexes.items():
            key = getattr(obj, idx_name, None)
            if btree.has_key(key):
                tree_set = btree[key]
                tree_set.remove(obj)

    @property
    def is_initialized(self):
        return self.root is not None

    @property
    def population(self):
        return self.data['population']

    @property
    def indexes(self):
        return self.data['indexes']


class ZodbDaoMeta(type(Dao)):
    def __new__(typ, name, bases, dct):
        cls = super().__new__(typ, name, bases, dct)
        if cls.__collection__ is not None:
            cls.collection = ZodbCollection(
                cls.__collection__(),
                cls.__schema__(),
                cls.__specs__(),
            )
        return cls


class ZodbDao(Dao, metaclass=ZodbDaoMeta):

    local = threading.local()
    local.storage = None
    local.db = None
    local.conn = None
    local.root = None

    class IndexSpec(object):
        """specification for a BTree index"""
        def __init__(self, field_name, unique=False):
            self.field_name = field_name
            self.unique = unique

    @staticmethod
    def __collection__() -> Text:
        return None

    @staticmethod
    def __specs__() -> Set[IndexSpec]:
        return {}

    @staticmethod
    def __schema__() -> Schema:
        return None

    @classmethod
    def connect(cls, db_name: str):
        if cls.local.storage is None:
            cls.local.storage = ZODB.FileStorage.FileStorage(db_name)
            cls.local.db = ZODB.DB(cls.local.storage)
            cls.local.conn = cls.local.db.open()
            cls.local.root = cls.local.conn.root()

    @classmethod
    def commit(cls):
        cls._raise_on_not_connected()
        cls.local.conn.transaction_manager.commit()

    @classmethod
    def rollback(cls):
        cls._raise_on_not_connected()
        cls.local.conn.transaction_manager.abort()

    @classmethod
    def _raise_on_not_connected(cls):
        if cls.local.storage is None:
            raise Exception('ZodbDao.connect() not called')

    @classmethod
    def _to_dict(cls, obj):
        record = {'_id': getattr(obj, '_id')}
        for field in cls.collection.schema.fields.values():
            key = field.load_key
            record[key] = getattr(obj, key, None)
        return record

    def __init__(self, *args, **kwargs):
        self._raise_on_not_connected()
        if not self.collection.is_initialized:
            self.collection.initialize(self.local.root, clear=False)

    def query(self, predicate, first=False, as_dict=True):
        results = list(self._query_dfs(predicate))
        if first:
            if results:
                return self._to_dict(results[0]) if as_dict else results[0]
            return None
        else:
            if as_dict:
                return [self._to_dict(result) for result in results]
            else:
                return results

    def _query_dfs(self, pred, empty=BTrees.OOBTree.TreeSet()):
        if isinstance(pred, ConditionalPredicate):
            index = self.collection.indexes[pred.attr_name]

            if pred.op == '=':
                if isinstance(pred.value, (list, tuple, set)):
                    return reduce(
                        BTrees.OOBTree.union,
                        (index[k] for k in pred.value)
                    )
                else:
                    return index.get(pred.value, empty)

            if pred.op == '!=':
                if isinstance(pred.value, (list, tuple, set)):
                    return reduce(
                        BTrees.OOBTree.union,
                        (
                            v for k, v in index.items()
                            if k not in pred.value
                        )
                    )
                else:
                    return reduce(
                        BTrees.OOBTree.union,
                        (v for k, v in index.items() if k != pred.value)
                    )

            result = empty
            key_slice = None

            if pred.op == '>=':
                keys = np.array(index.keys(), dtype=object)
                offset = bisect.bisect_left(keys, pred.value)
                key_slice = slice(offset, None, 1)
            elif pred.op == '>':
                keys = np.array(index.keys(), dtype=object)
                offset = bisect.bisect(keys, pred.value)
                key_slice = slice(offset, None, 1)
            elif pred.op == '<':
                keys = np.array(index.keys(), dtype=object)
                offset = bisect.bisect_left(keys, pred.value)
                key_slice = slice(0, offset, 1)
            elif pred.op == '<=':
                keys = np.array(index.keys(), dtype=object)
                offset = bisect.bisect(keys, pred.value)
                key_slice = slice(0, offset, 1)
            if key_slice:
                tree_sets = [
                    index[k] for k in keys[key_slice] if k is not None
                ]
                if tree_sets:
                    result = reduce(BTrees.OOBTree.union, tree_sets)
            return result

        elif isinstance(pred, BooleanPredicate):
            if pred.op == '&':
                operation = BTrees.OOBTree.intersection
                left_result = self._query_dfs(pred.lhs)
            elif pred.op == '|':
                operation = BTrees.OOBTree.union
            left_result = self._query_dfs(pred.lhs)
            if left_result:
                return operation(left_result, query_dfs(pred.rhs))
            else:
                return left_result  # the empty set

    def exists(self, _id=None, public_id=None) -> bool:
        if _id is not None:
            return _id in self.collection.population
        if public_id and 'public_id' in self.collection.indexes:
            return public_id in self.collection.indexes['public_id']
        return False

    def fetch(self, _id=None, public_id=None, fields=None, as_dict=True):
        record = None
        if _id is not None:
            obj = self.collection.population.get(_id)
            if obj is not None:
                record = self._to_dict(obj) if as_dict else obj
        else:
            predicate = ConditionalPredicate('public_id', '=', public_id)
            record = self.query(predicate, first=True, as_dict=as_dict)
        return record

    def fetch_many(
        self, _ids: list=None, public_ids: list=None, fields: dict=None
    ) -> dict:
        results = {}
        if _ids:
            results = {
                _id: self.fetch(_id=_id, fields=fields, as_dict=False)
                for _id in _ids
            }
        if public_id:
            results = {
                public_id: self.fetch(
                    public_id=public_id, fields=fields, as_dict=False
                )
                for public_id in public_ids
            }
        return [self._to_dict(r) for r in results]

    def create(self, _id=None, public_id=None, record: dict=None):
        record = record or {}
        record['_id'] = _id
        record['public_id'] = public_id or record.get('public_id')
        zodb_obj = ZodbObject(record, schema=self.collection.schema)
        self.collection.insert(zodb_obj)
        return self._to_dict(zodb_obj)

    def update(self, _id=None, public_id=None, data: dict=None) -> dict:
        zodb_obj = self.fetch(_id=_id, public_id=public_id, as_dict=False)
        self.collection.update(zodb_obj, data)
        return self._to_dict(zodb_obj)

    def update_many(
        self, _ids: list=None, public_ids: list=None, data: list=None
    ) -> dict:
        pass

    def delete(self, _id=None, public_id=None) -> dict:
        if _id is not None:
            obj = self.fetch(_id=_id, as_dict=False)
            if obj is None:
                raise Exception('not found')
            self.collection.remove(obj)
        elif public_id:
            raise NotImplementedError()


    def delete_many(self, _ids: list=None, public_ids: list=None) -> dict:
        pass
