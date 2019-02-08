import uuid

import ujson

from typing import Type, Dict, List
from copy import deepcopy

from redis import StrictRedis
from appyratus.utils import StringUtils

from pybiz.schema import fields
from pybiz.json import JsonEncoder
from pybiz.predicate import (
    Predicate,
    ConditionalPredicate,
    BooleanPredicate,
    OP_CODE,
)

from ..base import Dao
from .redis_types import HashSet, StringIndex, NumericIndex


class RedisDao(Dao):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.redis = None
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

    def bind(self, biz_type: Type['BizObject']):
        super().bind(biz_type)
        self.type_name = StringUtils.snake(biz_type.__name__).lower()
        self.redis = StrictRedis()
        self.records = HashSet(self.redis, self.type_name)
        self.revs = HashSet(self.redis, f'{self.type_name}_revisions')
        self.indexes = {}

        for k, field in biz_type.schema.fields.items():
            index_name = '{}:{}'.format(self.type_name, k.lower())
            index_type = self.field_2_index_type.get(field.__class__)
            if index_type is None:
                index_type = StringIndex
            self.indexes[k] = index_type(self.redis, index_name)

    def exists(self, _id) -> bool:
        return (_id in self.records)

    def count(self) -> int:
        return len(self.records)

    def fetch(self, _id, fields: Set[Text] = None) -> Dict:
        record_json = self.records.get(_id)
        if not record_json:
            return None

        full_record = JsonEncoder.decode(record_json)
        fields = fields if isinstance(fields, set) else set(fields or [])

        if fields:
            fields.update(['_id', '_rev'])
            return {k: full_record.get(k) for k in fields}
        else:
            return full_record

    def fetch_many(self, _ids: List, fields: Set[Text] = None) -> Dict:
        fields = fields if isinstance(fields, set) else set(fields or [])
        records_json = self.records.get_many(_ids)
        records = []

        if fields:
            for record_json in records_json:
                full_record = JsonEncoder.decode(record_json)
                records.append({
                    k: full_record.get(k) for k in fields
                })
        else:
            records = [
                JsonEncoder.decode(record_json) for record_json
                in self.records.get_many(_ids)
            ]

        return records

    def fetch_all(self, fields: Set[Text] = None) -> Dict:
        fields = fields if isinstance(fields, set) else set(fields or [])
        keys_to_remove = self.biz_type.schema.fields.keys() - fields
        return remove_keys(self.records, keys_to_remove, in_place=False)

    def upsert(self, record: Dict) -> Dict:
        _id = self.create_id(record)
        _rev = self.revs.increment(_id, delta=1)

        upserted_record = deepcopy(record)
        upserted_record.pop('_rev', None)
        upserted_record['_id'] = _id

        for k, v in record.items():
            self.indexes[k].upsert(_id, v)

        upserted_record['_rev'] = _rev
        return upserted_record

    def create(self, data: Dict) -> Dict:
        return self.upsert(data)

    def update(self, _id, record: Dict) -> Dict:
        return self.upsert(record)

    def create_many(self, records: List[Dict]) -> Dict:
        return [self.upsert(record) for record in records]

    def update_many(self, _ids: List, data: Dict = None) -> Dict:
        return [self.upsert(record) for record in records]

    def delete(self, _id) -> None:
        if self.records.delete(_id):
            for index in self.indexes.values():
                index.delete(_id)

    def delete_many(self, _ids: List) -> None:
        if self.records.delete_many(_ids):
            for index in self.indexes:
                index.delete_many(_ids)

    def query(self, predicate: 'Predicate', **kwargs):
        records = []

        if predicate is None:
            for _id, record_json in self.record.items():
                record = JsonEncoder.decode(record_json)
                record['_id'] = _id.decode()
                records.append(record)
        else:
            ids = self.query_ids(predicate)
            for _id in ids:
                record = JsonEncoder.decode(self.records[_id])
                record['_id'] = _id.decode()
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
                raise ValueError(
                    'unrecognized op: {}'.format(predicate.op)
                )
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
                raise ValueError(
                    'unrecognized op: {}'.format(predicate.op)
                )

        return ids


if __name__ == '__main__':
    from pybiz.api.repl import ReplRegistry
    from pybiz.biz import BizObject
    from pybiz.schema import fields

    class User(BizObject):
        name = fields.String()
        age = fields.Int()

    dao = RedisDao()
    dao.bind(User)

    for k in dao.redis.keys():
        dao.redis.delete(k)

    dg = dao.create({'name': 'a', 'age': 34})
    jd = dao.create({'name': 'a', 'age': 36})
    kc = dao.create({'name': 'b', 'age': 43})

    def test_1():
        return dao.query(User._id == dg['_id'])

    def test_2():
        return dao.query(User._id != dg['_id'])

    def test_3():
        return dao.query(User._id != kc['_id'])

    def test_4():
        return dao.query(User._id == jd['_id'])

    def test_5():
        return dao.query(User._id > '0'*32)

    def test_6():
        return dao.query(User._id >= dg['_id'])

    def test_7():
        return dao.query(User._id > dg['_id'])

    def test_8():
        return dao.query(User._id < dg['_id'])

    def test_9():
        return dao.query(User._id <= dg['_id'])

    def test_10():
        return dao.query(User._id <= jd['_id'])

    def test_11():
        return dao.query(User._id >= kc['_id'])

    def test_12():
        return dao.query(User.name <= dg['name'])

    def test_13():
        return dao.query(User.name > dg['name'])

    def test_14():
        return dao.query(User.name == dg['name'])

    def test_15():
        return dao.query(User.name != dg['name'])

    def test_16():
        return dao.query(User.name < kc['name'])

    def test_17():
        return dao.query(
            (User.name < kc['name']) & (User._id == dg['_id'])
        )

    def test_18():
        return dao.query(
            (User.name == kc['name']) | (User._id == dg['_id'])
        )

    def test_19():
        return dao.query(User.name.includes([dg['name']]))

    def test_20():
        return dao.query(User.name.excludes([dg['name']]))

    def test_21():
        return dao.query(User.name.excludes([kc['name']]))

    def test_22():
        return dao.query(User.name.includes([kc['name']]))

    for k, v in dao.records.items():
        print(k, v)

    repl = ReplRegistry()
    repl.manifest.process()
    repl.start({
        'User': User,
        'dao': dao,
        'dg': dg,
        'kc': kc,
        'jd': jd,
        'tests': [
            test_1,
            test_2,
            test_3,
            test_4,
            test_5,
            test_6,
            test_7,
            test_8,
            test_9,
            test_10,
            test_11,
            test_12,
            test_13,
            test_14,
            test_15,
            test_16,
            test_17,
            test_18,
            test_19,
            test_20,
            test_21,
            test_22,
        ]
    })
