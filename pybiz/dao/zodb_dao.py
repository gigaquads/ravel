import threading
import bisect

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
from pybiz.util import is_bizobj

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
    def __init__(self, name: str, schema: Schema, zodb_fields=None):
        self.name = name
        self.schema = schema
        self.zodb_fields = {s.name: s for s in (zodb_fields or [])}
        self.defaults = {
            s.name: s.default if callable(s.default) else lambda: s.default
            for s in self.zodb_fields.values() if s.default
        }
        self.root = None  # set in initialize
        self.data = None  # set in initialize

    def initialize(self, root: PersistentMapping, clear=False):
        self.root = root
        self.data = self.root.get(self.name)
        self.zodb_fields['_id'] = ZodbDao.Field('_id', unique=True)
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

    def insert(self, record: dict):
        for name, default in self.defaults.items():
            if record.get(name) is None:
                record[name] = default()
        _id = record.get('_id')
        assert _id is not None
        if self.population.has_key(_id):
            raise Exception('unique constraint')
        obj = ZodbObject(record, schema=self.schema)
        self.population[_id] = obj
        self._insert_to_indexes(obj)
        return record

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
            zodb_field = self.zodb_fields.get(idx_name)
            tree_set = btree.get(key)
            if zodb_field and zodb_field.unique and tree_set:
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
                cls.__fields__(),
            )
        return cls


class ZodbDao(Dao, metaclass=ZodbDaoMeta):

    local = threading.local()
    local.storage = None
    local.db = None
    local.conn = None
    local.root = None

    class Field(object):
        def __init__(self, name, default=None, unique=False):
            self.name = name
            self.default = default
            self.unique = unique

    @staticmethod
    def __collection__() -> Text:
        return None

    @staticmethod
    def __fields__() -> Set[Field]:
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
    def to_dict(cls, obj, whitelist: Set[Text] = None):
        record = {'_id': getattr(obj, '_id')}
        for field in cls.collection.schema.fields.values():
            key = field.load_key
            if (whitelist is None) or (key in whitelist):
                record[key] = getattr(obj, key, None)
        return record

    def __init__(self, *args, **kwargs):
        self._raise_on_not_connected()
        if not self.collection.is_initialized:
            self.collection.initialize(self.local.root, clear=False)

    def query(
        self,
        predicate,
        fields=None,
        order_by=None,
        first=False,
        as_dict=True,
        **kwargs
    ):
        zodb_objects = list(self._query_dfs(predicate))
        data = None

        if not zodb_objects:
            return None if first else []

        # return only first result in result list
        if first:
            obj = zodb_objects[0]
            return self.to_dict(obj, fields) if as_dict else obj

        # sort the results
        for name, sort_dir in (order or []):
            reverse = (sort_dir == -1)  # -1 means DESC
            cmp_key = lambda obj: getattr(obj, name, None)
            zodb_objects = sorted(zodb_objects, key=cmp_key, reverse=reverse)

        # convert ZodbObjects to dicts
        if as_dict:
            return [self.to_dict(obj, fields) for obj in zodb_objects]
        else:
            return zodb_objects

    def _query_dfs(self, pred, empty=BTrees.OOBTree.TreeSet()):
        if isinstance(pred, ConditionalPredicate):
            index = self.collection.indexes[pred.attr_name]

            if pred.op == '=':
                if isinstance(pred.value, (list, tuple, set)):
                    sets = [index[k] for k in pred.value]
                    return self._reduce(BTrees.OOBTree.union, sets)
                else:
                    return index.get(pred.value, empty)

            if pred.op == '!=':
                if isinstance(pred.value, (list, tuple, set)):
                    sets = [v for k, v in index.items() if k not in pred.value]
                else:
                    sets = [v for k, v in index.items() if k != pred.value]
                return self._reduce(BTrees.OOBTree.union, sets)

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
                result = self._reduce(BTrees.OOBTree.union, [
                    index[k] for k in keys[key_slice] if k is not None
                ])
            return result

        elif isinstance(pred, BooleanPredicate):
            if pred.op == '&':
                lhs_result = self._query_dfs(pred.lhs)
                if lhs_result:
                    rhs_result = self._query_dfs(pred.rhs)
                    return BTrees.OOBTree.intersection(lhs_result, rhs_result)
            elif pred.op == '|':
                lhs_result = self._query_dfs(pred.lhs)
                rhs_result = self._query_dfs(pred.rhs)
                return BTrees.OOBTree.union(lhs_result, rhs_result)
            else:
                raise Exception('unrecognized boolean predicate')

    def _reduce(self, func, sequences):
        if sequences:
            if len(sequences) == 1:
                return sequences[0]
            else:
                return reduce(func, sequences)
        else:
            return BTrees.OOBTree.TreeSet()

    def exists(self, _id) -> bool:
        return _id in self.collection.population

    def fetch(self, _id, fields=None, as_dict=True):
        obj = self.collection.population.get(_id)
        if obj is not None and as_dict:
            return self.to_dict(obj, fields)
        return obj

    def fetch_many(self, _ids: list, fields: dict=None) -> dict:
        results = {
            _id: self.fetch(_id=_id, fields=fields, as_dict=False)
            for _id in _ids
        }
        return [self.to_dict(r) for r in results]

    def create(self, _id, record: dict):
        record = dict(record or {}, _id=_id)
        return self.collection.insert(record)

    def update(self, _id, data: dict) -> dict:
        zodb_obj = self.fetch(_id=_id, as_dict=False)
        if zodb_obj is not None:
            self.collection.update(zodb_obj, data)
            return self.to_dict(zodb_obj)
        else:
            raise Exception('not found')

    def update_many(self, _ids: list, data: list) -> dict:
        zodb_objects = self.fetch_many(_ids=_ids)
        for obj, changes in zip(zodb_objects, data):
            if obj is not None:
                self.collection.update(obj, changes)

    def delete(self, _id) -> dict:
        obj = self.fetch(_id=_id, as_dict=False)
        if obj is not None:
            self.collection.remove(obj)
        else:
            raise Exception('not found')

    def delete_many(self, _ids: list) -> dict:
        zodb_objects = self.fetch_many(_ids=_ids)
        for obj in zodb_objects:
            if obj is not None:
                self.collection.remove(obj)
            else:
                raise Exception('not found')
