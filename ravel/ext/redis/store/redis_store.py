from typing import Type, Dict, List, Set, Text
from collections import defaultdict
from copy import deepcopy

from redis import StrictRedis
from appyratus.utils.string_utils import StringUtils

from ravel.store import Store
from ravel.schema import fields
from ravel.constants import ID, REV
from ravel.util.json_encoder import JsonEncoder
from ravel.query.predicate import (
    Predicate,
    ConditionalPredicate,
    BooleanPredicate,
    OP_CODE,
)

from .redis_types import RedisClient, HashSet, StringIndex, NumericIndex


class RedisStore(Store):

    redis = None
    host = 'localhost'
    port = 6379
    db = 0

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.encoder = JsonEncoder()
        self.field_2_index_type = {
            fields.String: StringIndex,
            fields.Int: NumericIndex,
            fields.Float: NumericIndex,
            fields.DateTime: NumericIndex,
            fields.Timestamp: NumericIndex,
            fields.Uuid: NumericIndex,
            fields.Bool: NumericIndex,
        }

    @classmethod
    def on_bootstrap(cls, host=None, port=None, db=None):
        cls.host = host or cls.env.REDIS_HOST or cls.host
        cls.port = port or cls.env.REDIS_PORT or cls.port
        cls.db = db if db is not None else (cls.env.REDIS_DB or cls.db)
        cls.redis = RedisClient(host=cls.host, port=cls.port, db=cls.db)

    def on_bind(self, resource_type: Type['Resource'], **kwargs):
        self.type_name = StringUtils.snake(resource_type.__name__).lower()
        self.records = HashSet(self.redis, self.type_name)
        self.revs = HashSet(self.redis, f'{self.type_name}_revisions')
        self.indexes = self._bind_indexes()

    def _bind_indexes(self):
        indexes = {}
        for k, field in self.resource_type.Schema.fields.items():
            index_name = f'{self.type_name}:{k.lower()}'
            index_type = self.field_2_index_type.get(field.__class__)
            if index_type is None:
                index_type = StringIndex
            indexes[k] = index_type(self.redis, index_name)
        return indexes

    def exists(self, _id) -> bool:
        return (_id in self.records)

    def exists_many(self, _ids: Set) -> Dict[object, bool]:
        return {
            _id: (_id in self.records)
            for _id in _ids
        }

    def count(self) -> int:
        return len(self.records)

    def fetch(self, _id, fields: Set[Text] = None) -> Dict:
        fields = fields if isinstance(fields, set) else set(fields or [])

        pipe = self.redis.pipeline()

        self.records.get(_id)
        self.revs.get(_id)

        record_json, rev_str = pipe.execute()

        if not record_json:
            return None

        full_record = JsonEncoder.decode(record_json)
        full_record[REV] = int(rev_str) - 1

        if fields:
            return {k: full_record.get(k) for k in fields}
        else:
            return full_record

    def fetch_many(self, _ids: List, fields: Set[Text] = None) -> Dict:
        pipe = self.redis.pipeline()

        self.records.get_many(_ids, pipe=pipe)
        self.revs.get_many(_ids, pipe=pipe)

        json_records, rev_strs = pipe.execute()

        fields = fields if isinstance(fields, set) else set(fields or [])
        records = {}

        if fields:
            for record_json, _rev in zip(json_records, rev_strs):
                record = JsonEncoder.decode(record_json)
                record = {k: record.get(k) for k in fields}
                record[REV] = int(_rev) - 1
                records[record[ID]] = record
        else:
            for record_json, _rev in zip(json_records, rev_strs):
                record = JsonEncoder.decode(record_json)
                record[REV] = int(_rev) - 1
                records[record[ID]] = record

        return records

    def fetch_all(self, fields: Set[Text] = None) -> Dict:
        return self.fetch_many(list(self.records.keys()), fields=fields)

    def upsert(self, record: Dict, pipe=None) -> Dict:
        is_creating = record.get(ID) is None
        _id = self.create_id(record)

        # prepare the record for upsert
        upserted_record = record.copy()
        upserted_record[ID] = _id

        # json encode and store record JSON
        self.records[_id] = self.encoder.encode(upserted_record)

        # update indexes
        for k, v in record.items():
            self.indexes[k].upsert(_id, v)

        # add rev to record AFTER insert to avoid storing _rev in records hset
        if is_creating:
            upserted_record[REV] = 0
        else:
            upserted_record[REV] = self.revs.increment(_id)

        return upserted_record

    def create(self, data: Dict) -> Dict:
        pipe = self.redis.pipeline()
        created_record = self.upsert(data)
        pipe.execute()
        return created_record

    def update(self, _id, record: Dict) -> Dict:
        pipe = self.redis.pipeline()
        updated_record = self.upsert(record, pipe=pipe)
        pipe.execute()
        return updated_record

    def create_many(self, records: List[Dict]) -> Dict:
        pipe = self.redis.pipeline()
        created_records = [self.upsert(rec, pipe=pipe) for rec in records]
        pipe.execute()
        return created_records

    def update_many(self, _ids: List, data: Dict = None) -> Dict:
        pipe = self.redis.pipeline()
        updated_records = [self.upsert(rec, pipe=pipe) for rec in records]
        pipe.execute()
        return updated_records

    def delete(self, _id) -> None:
        pipe = self.redis.pipeline()
        if self.records.delete(_id, pipe=pipe):
            for index in self.indexes.values():
                index.delete(_id, pipe=pipe)
        pipe.execute()

    def delete_many(self, _ids: List) -> None:
        pipe = self.redis.pipeline()
        if self.records.delete_many(_ids, pipe=pipe):
            self.revs.delete_many(_ids)
            for index in self.indexes.values():
                index.delete_many(_ids, pipe=pipe)
        pipe.execute()

    def delete_all(self):
        if self.count():
            self.delete_many(self.records.keys())

    def query(self, predicate: 'Predicate', **kwargs):
        if predicate is None:
            _ids = self.records.keys()
        else:
            _ids = self.query_ids(predicate)

        records = []

        if _ids:
            _id_field = self.resource_type.Schema.fields[ID]
            json_records = self.records.get_many(_ids)
            rev_strs = self.revs.get_many(_ids)
            for json_record, rev_str in zip(json_records, rev_strs):
                record = JsonEncoder.decode(json_record)
                record[REV] = int(rev_str.decode())
                records.append(record)

        return records

    def query_ids(self, predicate, pipeline=None):
        # TODO: use a redis pipeline object

        if isinstance(predicate, ConditionalPredicate):
            index = self.indexes.get(predicate.field.name)
            if index is None:
                # TODO: raise custom exception
                raise Exception('no index')

            ids = set()
            if predicate.op == OP_CODE.EQ:
                ids = set(index.search(
                    upper=predicate.value,
                    lower=predicate.value,
                    include_upper=True,
                    include_lower=True
                ))
            elif predicate.op == OP_CODE.NEQ:
                ids = set(index.search(
                    upper=predicate.value,
                    include_upper=False,
                    include_lower=True
                ))
                ids |= set(index.search(
                    lower=predicate.value,
                    include_upper=True,
                    include_lower=False
                ))
            elif predicate.op == OP_CODE.GEQ:
                ids = set(index.search(
                    lower=predicate.value,
                    include_lower=True
                ))
            elif predicate.op == OP_CODE.LEQ:
                ids = set(index.search(
                    upper=predicate.value,
                    include_upper=True
                ))
            elif predicate.op == OP_CODE.LT:
                ids = set(index.search(
                    upper=predicate.value,
                    include_upper=False
                ))
            elif predicate.op == OP_CODE.GT:
                ids = set(index.search(
                    lower=predicate.value,
                    include_lower=False
                ))
            elif predicate.op == OP_CODE.INCLUDING:
                ids = set()
                for v in predicate.value:
                    ids |= set(index.search(
                        upper=v,
                        lower=v,
                        include_upper=True,
                        include_lower=True
                    ))
            elif predicate.op == OP_CODE.EXCLUDING:
                ids = set()
                for v in predicate.value:
                    ids |= set(index.search(
                        upper=v,
                        include_upper=False,
                        include_lower=True
                    ))
                    ids |= set(index.search(
                        lower=v,
                        include_upper=True,
                        include_lower=False
                    ))
            else:
                raise ValueError('unrecognized op: {}'.format(predicate.op))
        elif isinstance(predicate, BooleanPredicate):
            ids_lhs = self.query_ids(predicate.lhs)
            if ids_lhs:
                ids_rhs = self.query_ids(predicate.rhs)
            else:
                ids_rhs = set()
            if predicate.op == OP_CODE.OR:
                ids = ids_lhs | ids_rhs
            elif predicate.op == OP_CODE.AND:
                ids = ids_lhs & ids_rhs
            else:
                raise ValueError('unrecognized op: {}'.format(predicate.op))

        return ids
