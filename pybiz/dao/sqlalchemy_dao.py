import re

import sqlalchemy as sa

from copy import deepcopy
from typing import List, Dict, Text
from threading import local

from appyratus.decorators import memoized_property

from pybiz.predicate import ConditionalPredicate, BooleanPredicate

from .base import Dao


class SqlalchemyDao(object):

    local = local()
    local.engine = None
    local.connection = None

    @staticmethod
    def __table__():
        raise NotImplementedError()

    @classmethod
    def initialize(cls, url: Text, metadata=None, echo=False):
        if cls.local.engine is not None:
            cls.local.engine.dispose()
        cls.local.metadata = metadata or sa.MetaData()
        cls.local.engine = sa.create_engine(url, echo=echo)

    @classmethod
    def create_tables(cls):
        cls.local.metadata.create_all(cls.local.engine)

    @classmethod
    def connect(cls):
        cls.local.connection = cls.local.engine.connect()

    @classmethod
    def close(cls):
        cls.local.connection.close()

    @classmethod
    def begin(cls):
        cls.local.trans = cls.local.connection.begin()

    @classmethod
    def commit(cls):
        cls.local.trans.commit()
        cls.local.trans = None

    @classmethod
    def rollback(cls):
        cls.local.connection.rollback()

    @memoized_property
    def table(self):
        return self.__table__()

    @property
    def conn(self):
        return self.local.connection

    @property
    def supports_returning(self):
        return self.local.engine.dialect.implicit_returning

    def __init__(self):
        self.table.metadata.bind = self.local.engine

    def query(
        self,
        predicate,
        fields=None,
        order_by=None,
        **kwargs,
    ):
        fields = fields or self.table.c.keys()
        filters = self._prepare_predicate(predicate)
        columns = [getattr(self.table.c, k) for k in fields]
        query = sa.select(columns).where(filters)
        cursor = self.conn.execute(query)
        results = []
        while True:
            page = [dict(row.items()) for row in cursor.fetchmany(256)]
            if page:
                results.extend(page)
            else:
                return results

    def _prepare_predicate(self, pred, empty=set()):
        if isinstance(pred, ConditionalPredicate):
            col = getattr(self.table.c, pred.attr_name)
            if pred.op == '=':
                if isinstance(pred.value, (list, tuple, set)):
                    return col.in_(pred.value)
                else:
                    return col == pred.value
            elif pred.op == '!=':
                if isinstance(pred.value, (list, tuple, set)):
                    return sa.not_(col.in_(pred.value))
                else:
                    return col != pred.value
            if pred.op == '>=':
                return col >= pred.value
            elif pred.op == '>':
                return col > pred.value
            elif pred.op == '<':
                return col < pred.value
            elif pred.op == '<=':
                return col <= pred.value
            else:
                raise Exception('unrecognized conditional predicate')
        elif isinstance(pred, BooleanPredicate):
            if pred.op == '&':
                lhs_result = self._query_dfs(pred.lhs)
                rhs_result = self._query_dfs(pred.rhs)
                return sa.and_(lhs_result, rhs_result)
            elif pred.op == '|':
                lhs_result = self._query_dfs(pred.lhs)
                rhs_result = self._query_dfs(pred.rhs)
                return sa.or_(lhs_result, rhs_result)
            else:
                raise Exception('unrecognized boolean predicate')
        else:
            raise Exception('unrecognized predicate type')

    def exists(self, _id) -> bool:
        columns = [sa.func.count(self.table.c._id)]
        query = (
            sa.select(columns)
                .where(self.table.c._id == _id)
        )
        result = self.conn.execute(query)
        return bool(result.scalar())

    def fetch(self, _id, fields=None) -> Dict:
        records = self.fetch_many(_ids=[_id], fields=fields)
        return records[_id] if records else None

    def fetch_many(self, _ids: List, fields=None) -> Dict:
        # TODO: get field names from __schema__
        fields = set(fields or self.table.c.keys()) | {'_id'}
        columns = [getattr(self.table.c, k) for k in fields]
        select_stmt = sa.select(columns).where(self.table.c._id.in_(_ids))
        cursor = self.conn.execute(select_stmt)
        records = {}
        while True:
            page = cursor.fetchmany(256)
            if page:
                for row in page:
                    records[row._id] = dict(row.items())
            else:
                return records

    def create(self, record: dict) -> dict:
        insert_stmt = self.table.insert().values(**record)
        import ipdb; ipdb.set_trace()

        if self.supports_returning:
            insert_stmt = insert_stmt.return_defaults()
            result = self.conn.execute(insert_stmt)
            return dict(record, **(result.returned_defaults or {}))
        else:
            result = self.conn.execute(insert_stmt)
            return self.fetch(_id=result.lastrowid)

    def create_many(self, records: List[Dict]) -> None:
        self.conn.execute(self.table.insert(), records)

    def update(self, _id, data: Dict) -> Dict:
        update_stmt = (
            self.table
                .update()
                .values(**data)
                .where(self.table.c._id == _id)
            )
        if self.supports_returning:
            update_stmt = update_stmt.return_defaults()
            result = self.conn.execute(update_stmt)
            return dict(data, **(result.returned_defaults or {}))
        else:
            return None

    def update_many(self, _ids: List, data: Dict = None) -> None:
        update_stmt = (
            self.table
                .update()
                .values(**data)
                .where(self.table.c._id.in_(_ids))
            )
        self.conn.execute(update_stmt)

    def delete(self, _id) -> None:
        delete_stmt = self.table.delete().where(self.table.c._id == _id)
        self.conn.execute(delete_stmt)

    def delete_many(self, _ids: list) -> None:
        delete_stmt = self.table.delete().where(self.table.c._id.in_(_ids))
        self.conn.execute(delete_stmts)

if __name__ == '__main__':
    import json

    from pybiz.biz import BizObject, Relationship
    from appyratus.validation import Schema, fields

    meta = sa.MetaData()

    t_user = sa.Table('user', meta, *[
        sa.Column('_id', sa.Integer(), primary_key=True),
        sa.Column('name', sa.String(), default='Barb')
    ])

    t_dog = sa.Table('dog', meta, *[
        sa.Column('_id', sa.Integer(), primary_key=True),
        sa.Column('owner_id', sa.Integer(), index=True),
        sa.Column('breed', sa.String())
    ])

    t_cat = sa.Table('cat', meta, *[
        sa.Column('_id', sa.Integer(), primary_key=True),
        sa.Column('friend_id', sa.Integer(), index=True),
        sa.Column('color', sa.String())
    ])

    # Daos
    # ------------------------------------------------------------
    class UserDao(SqlalchemyDao):
        @staticmethod
        def __table__():
            return t_user

    class DogDao(SqlalchemyDao):
        @staticmethod
        def __table__():
            return t_dog

    class CatDao(SqlalchemyDao):
        @staticmethod
        def __table__():
            return t_cat

    # Schemas
    # ------------------------------------------------------------
    class UserSchema(Schema):
        name = fields.Str()

    class DogSchema(Schema):
        breed = fields.Str()
        owner_id = fields.Int(load_only=True)

    class CatSchema(Schema):
        color = fields.Str()
        friend_id = fields.Int(load_only=True)

    # BizObjects
    # ------------------------------------------------------------
    class User(BizObject):

        dogs = Relationship(
            target=lambda: Dog,
            query=lambda user, fields=None:
                Dog.query(Dog.owner_id == user._id, fields=fields),
            many=True,
        )

        @staticmethod
        def __dao__():
            return UserDao

        @staticmethod
        def __schema__():
            return UserSchema

    class Dog(BizObject):

        friend = Relationship(
            target=lambda: Cat,
            query=lambda dog, fields=None:
                Cat.query(Cat.friend_id == dog._id, fields=fields, first=True)
        )

        @staticmethod
        def __dao__():
            return DogDao

        @staticmethod
        def __schema__():
            return DogSchema

    class Cat(BizObject):

        @staticmethod
        def __dao__():
            return CatDao

        @staticmethod
        def __schema__():
            return CatSchema


    # Test scenario
    # ------------------------------------------------------------
    url = 'postgresql+psycopg2://postgres@0.0.0.0:5432/test'
    SqlalchemyDao.initialize(url, metadata=meta, echo=False)
    SqlalchemyDao.create_tables()

    SqlalchemyDao.connect()
    SqlalchemyDao.begin()

    jeff = User(name='Jeff').save()
    chewy = Dog(breed='half', owner_id=jeff._id).save()
    picard = Dog(breed='full', owner_id=jeff._id).save()
    kitty = Cat(color='red', friend_id=chewy._id).save()

    picard.breed = 'foo'
    picard.save()

    chewy.breed = 'baz'
    chewy.save()

    user = User.get(_id=jeff._id, fields=[
        'name', {'dogs': ['breed', {'friend': ['color']}]}
    ])

    print(json.dumps(user.dump(), indent=2, sort_keys=True))
    print(json.dumps(Dog.get(1).dump(), indent=2, sort_keys=True))
    print(json.dumps(Dog.get(2).dump(), indent=2, sort_keys=True))

    print(json.dumps(
        [x.dump() for x in User.get_many([1, 2, 3], as_list=True)],
        indent=2, sort_keys=True)
    )

    SqlalchemyDao.commit()
    SqlalchemyDao.close()
