import uuid

import ujson

from typing import Type, Dict, List

from redis import StrictRedis

from pybiz.schema import fields
from pybiz.util import JsonEncoder
from pybiz.predicate import (
    Predicate,
    ConditionalPredicate,
    BooleanPredicate,
    OP_CODE,
)

from ..base import Dao
from .redis_types import HashSet, StringIndex, NumericIndex


#TODO: move next_id into Dao base class

class RedisDao(Dao):

    @classmethod
    def next_id(self):
        return uuid.uuid4().hex

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.redis = StrictRedis()
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

    def bind(self, bizobj_type: Type['BizObject']):
        super().bind(bizobj_type)
        self.type_name = bizobj_type.__name__.lower()
        self.records = HashSet(self.redis, self.type_name)
        self.indexes = {}

        for k, field in bizobj_type.schema.fields.items():
            index_name = '{}:{}'.format(self.type_name, k.lower())
            index_type = self.field_2_index_type.get(field.__class__)
            if index_type is None:
                index_type = StringIndex
            self.indexes[k] = index_type(self.redis, index_name)

    def exists(self, _id) -> bool:
        return (_id in self.records)

    def fetch(self, _id, fields: Dict = None) -> Dict:
        record_json = self.records.get(_id)
        if not record_json:
            return None

        full_record = ujson.loads(record_json)
        if fields:
            return {k: full_record[k] for k in fields}
        else:
            return full_record

    def fetch_many(self, _ids: List, fields: Dict = None) -> Dict:
        records_json = self.records.get_many(_ids)
        records = []

        if fields:
            for record_json in records_json:
                full_record = ujson.loads(record_json)
                records.append({
                    k: full_record[k] for k in fields
                })
        else:
            records = [
                ujson.loads(record_json) for record_json
                in self.records.get_many(_ids)
            ]

        return records

    def upsert(self, record: Dict) -> Dict:
        _id = record.pop('_id', None)
        if _id is None:
            _id = self.next_id()

        self.records[_id] = self.encoder.encode(record)
        for k, v in record.items():
            self.indexes[k].upsert(_id, v)

        self.indexes['_id'].upsert(_id, _id)
        record['_id'] = _id

        return record

    def create(self, data: Dict) -> Dict:
        return self.upsert(data)

    def update(self, _id, data: Dict) -> Dict:
        data['_id'] = _id
        return self.upsert(data)

    def create_many(self, records: List[Dict]) -> None:
        pass # TODO: impl

    def update_many(self, _ids: List, data: List[Dict] = None) -> None:
        pass

    def delete(self, _id) -> None:
        # TODO: needs testing
        if self.records.delete(_id):
            for index in self.indexes.values():
                index.delete(_id)

    def delete_many(self, _ids: List) -> None:
        # TODO: needs testing
        if self.records.delete_many(_ids):
            for index in self.indexes:
                index.delete_many(_ids)

    def query(self, predicate: 'Predicate', **kwargs):
        records = []

        if predicate is None:
            for _id, record_json in self.record.items():
                record = ujson.loads(record_json)
                record['_id'] = _id.decode()
                records.append(record)
        else:
            ids = self.query_ids(predicate)
            for _id in ids:
                record = ujson.loads(self.records[_id])
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
