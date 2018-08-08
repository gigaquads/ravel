import threading
import bisect
import inspect

import numpy as np
import persistent
import BTrees.OOBTree
import BTrees.IOBTree
import ZODB.FileStorage
import ZODB

from typing import Dict, List
from functools import reduce

from persistent.mapping import PersistentMapping
from appyratus.validation.schema import Schema

from pybiz.predicate import ConditionalPredicate, BooleanPredicate

from .base import Dao


class ZodbObject(persistent.Persistent):
    def __init__(self, record: Dict, schema: Schema = None):
        super().__init__()
        for k in (schema.fields if schema else record):
            setattr(self, k, record.get(k))

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


class ZodbDaoMeta(type(Dao)):
    def __new__(typ, name, bases, dct):
        cls = super().__new__(typ, name, bases, dct)
        if name != 'ZodbDao':
            cls.memoize()
        return cls


class ZodbDao(Dao, metaclass=ZodbDaoMeta):

    local = threading.local()
    local.storage = None
    local.db = None
    local.conn = None
    local.root = None

    _memoized_attrs = {}
    _memoization_lock = threading.RLock()

    class IndexSpec(object):
        """specification for a BTree index"""
        def __init__(self, index_type: type, unique=False):
            self.index_type = index_type
            self.unique = unique

    class OOBTreeIndex(IndexSpec):
        """index with string/generic object keys"""
        def __init__(self, **kwargs):
            super().__init__(BTrees.OOBTree.BTree, **kwargs)

    class IOBTreeIndex(IndexSpec):
        """index with integer keys"""
        def __init__(self, **kwargs):
            super().__init__(BTrees.IOBTree.BTree, **kwargs)

    @staticmethod
    def __collection__():
        return None

    @staticmethod
    def __object_type__():
        return ZodbObject

    @staticmethod
    def __schema__():
        raise NotImplementedError('override in subclass')

    @staticmethod
    def __indexes__():
        return {
            '_id': ZodbDao.OOBTreeIndex(unique=True),
            'public_id': ZodbDao.OOBTreeIndex(unique=True),
        }

    @classmethod
    def connect(cls, db_name: str):
        if cls.local.storage is None:
            cls.local.storage = ZODB.FileStorage.FileStorage(db_name)
            cls.local.db = ZODB.DB(cls.local.storage)
            cls.local.conn = cls.local.db.open()
            cls.local.root = cls.local.conn.root()

    @classmethod
    def root(cls):
        return cls.local.root

    @classmethod
    def commit(cls):
        if cls.local.conn:
            cls.local.conn.transaction_manager.commit()

    @classmethod
    def rollback(cls):
        if cls.local.conn:
            cls.local.conn.transaction_manager.abort()

    @classmethod
    def close(cls):
        if cls.local.db:
            cls.local.db.close()

    @classmethod
    def get_index(cls, attr_name: str):
        col = cls.get_collection()
        indexes = col.get('indexes')
        if indexes is None:
            indexes = PersistentMapping()
            col['indexes'] = indexes
        if attr_name not in indexes:
            index_spec = cls._memoized_attrs['indexes'][attr_name]
            if attr_name == '_id':
                indexes[attr_name] = cls.OOBTreeIndex(unique=True)
            else:
                indexes[attr_name] = index_spec.index_type()
        return indexes[attr_name]

    @classmethod
    def get_collection(cls):
        root = cls.root()
        col_name = cls._memoized_attrs['collection']
        if col_name not in root:
            root[col_name] = PersistentMapping()
        if col_name not in root:
            root[col_name] = PersistentMapping()
        return root[col_name]

    @classmethod
    def update_indexes(cls, model):
        index_specs = cls._memoized_attrs['indexes']
        for k, index_spec in index_specs.items():
            index = cls.get_index(k)
            v = getattr(model, k, None)
            if v is not None:
                if v not in index:
                    index[v] = BTrees.OOBTree.TreeSet()
                if index_spec.unique and index[v]:
                    # TODO: raise constraint error if already set
                    raise Exception('unique constraint violation')
                index[v].insert(model)

    @classmethod
    def next_id(cls):
        col = cls.get_collection()
        next_id = col.setdefault('id_counter', 1)
        col['id_counter'] = next_id + 1
        return str(next_id)

    @classmethod
    def to_dict(cls, model):
        schema = cls._memoized_attrs['schema']
        if schema is not None:
            attrs = schema.fields.keys()
            return {k: getattr(model, k) for k in schema.fields}
        else:
            return {
                k: v for k, v in inspect.getmembers(model)
                if not (hasattr(persistent.Persistent, k) or callable(v))
            }

    @classmethod
    def memoize(cls):
        if not cls._memoized_attrs:
            with cls._memoization_lock:
                schema_type = cls.__schema__
                cls._memoized_attrs = {
                    'collection': cls.__collection__() or cls.__name__,
                    'object_type': cls.__object_type__(),
                    'schema': schema_type() if schema_type else None,
                    'indexes': cls.__indexes__(),
                }

    @property
    def schema(self) -> Schema:
        return self._memoized_attrs['schema']

    @property
    def object_type(self) -> ZodbObject:
        return self._memoized_attrs['object_type']

    @property
    def indexes(self) -> List[IndexSpec]:
        return self._memoized_attrs['indexes']

    @property
    def collection(self) -> str:
        return self._memoized_attrs['collection']

    def query(self, predicate, first=False, as_dict=True):
        def query_dfs(pred, empty=BTrees.OOBTree.TreeSet()):
            if isinstance(pred, ConditionalPredicate):
                if pred.attr_name == '_id':
                    pred.value = str(pred.value)
                index = self.get_index(pred.attr_name)
                if pred.op == '=':
                    return index.get(pred.value, empty)
                result = empty
                key_slice = None
                if pred.op == '>=':
                    if pred.value <= index.maxKey():
                        keys = np.array(index.keys(), dtype=object)
                        offset = bisect.bisect_left(keys, pred.value)
                        key_slice = slice(offset, None, 1)
                elif pred.op == '>':
                    if pred.value < index.maxKey():
                        keys = np.array(index.keys(), dtype=object)
                        offset = bisect.bisect(keys, pred.value)
                        key_slice = slice(offset, None, 1)
                elif pred.op == '<':
                    if pred.value > index.minKey():
                        keys = np.array(index.keys(), dtype=object)
                        offset = bisect.bisect_left(keys, pred.value)
                        key_slice = slice(0, offset, 1)
                elif pred.op == '<=':
                    if pred.value >= index.minKey():
                        keys = np.array(index.keys(), dtype=object)
                        offset = bisect.bisect(keys, pred.value)
                        key_slice = slice(0, offset, 1)
                if key_slice:
                    result = reduce(
                        BTrees.OOBTree.union,
                        (index[k] for k in keys[key_slice])
                    )
                return result
            elif isinstance(pred, BooleanPredicate):
                if pred.op == '&':
                    operation = BTrees.OOBTree.intersection
                    left_result = query_dfs(pred.lhs)
                if pred.op == '|':
                    operation = BTrees.OOBTree.union
                left_result = query_dfs(pred.lhs)
                if left_result:
                    return operation(left_result, query_dfs(pred.rhs))
                else:
                    return left_result  # the empty set

        results = list(query_dfs(predicate))

        if first:
            if results:
                return self.to_dict(results[0]) if as_dict else results[0]
            return None
        else:
            if as_dict:
                return [self.to_dict(result) for result in results]
            else:
                return results

    def exists(self, _id=None, public_id=None) -> bool:
        pass

    def fetch(self, _id=None, public_id=None, fields=None, as_dict=True):
        if _id is not None:
            predicate = ConditionalPredicate('_id', '=', str(_id))
        else:
            predicate = ConditionalPredicate('public_id', '=', public_id)
        return self.query(predicate, first=True, as_dict=as_dict)

    def fetch_many(
        self, _ids: list=None, public_ids: list=None, fields: dict=None
    ) -> dict:
        results = set()
        if _ids:
            results |= {
                self.fetch(_id=_id, fields=fields, as_dict=False)
                for _id in _ids
            }
        if public_id:
            results |= {
                self.fetch(public_id=public_id, fields=fields, as_dict=False)
                for public_id in public_ids
            }
        return [self.to_dict(r) for r in results]

    def create(self, _id=None, public_id=None, record: dict=None):
        record = record or {}
        record['_id'] = _id if _id is not None else self.next_id()
        if public_id is not None:
            record['public_id'] = public_id
        model = self.object_type(record, schema=self.schema)
        self.update_indexes(model)
        return self.to_dict(model)

    def update(self, _id=None, public_id=None, data: dict=None) -> dict:
        pass

    def update_many(
        self, _ids: list=None, public_ids: list=None, data: list=None
    ) -> dict:
        pass

    def delete(self, _id=None, public_id=None) -> dict:
        pass

    def delete_many(self, _ids: list=None, public_ids: list=None) -> dict:
        pass


if __name__ == '__main__':
    import sys

    from appyratus.validation import fields
    from pybiz.biz import BizObject

    class UserSchema(Schema):
        _id = fields.Int()
        public_id = fields.Str(allow_none=True)
        name = fields.Str()

    class UserDao(ZodbDao):

        @staticmethod
        def __collection__():
            return 'things'

        @staticmethod
        def __schema__():
            return UserSchema

        @staticmethod
        def __indexes__():
            return {
                '_id': ZodbDao.OOBTreeIndex(unique=True),
                'public_id': ZodbDao.OOBTreeIndex(unique=True),
                'name': ZodbDao.OOBTreeIndex(unique=False),
            }

    class User(BizObject):
        @classmethod
        def __schema__(cls):
            return UserSchema

        @classmethod
        def __dao__(cls):
            return UserDao

    dao = UserDao()
    dao.connect('things.fs')

    if sys.argv[1] == 'create':
        dao.create(record={'name': 'Daniel'})
        dao.create(record={'name': 'Jeff'})
        dao.create(record={'name': 'KC'})
        dao.commit()
        dao.close()
    elif sys.argv[1] == 'fetch':
        print(User.get(_id=1))
        print(User.query((User._id > 2) & (User.name > 'Je')))
