import re

import sqlalchemy as sa

from copy import deepcopy
from typing import List, Dict, Text
from threading import local

from appyratus.decorators import memoized_property

from pybiz.predicate import ConditionalPredicate, BooleanPredicate

from .base import Dao


class SqlalchemyExpressionLanguageDao(object):

    local = local()
    local.engine = None
    local.connection = None

    @staticmethod
    def __table__():
        raise NotImplementedError()

    @classmethod
    def initialize(cls, url: Text, meta=None, echo=False):
        if cls.local.engine is not None:
            cls.local.engine.dispose()
        cls.local.metadata = meta or sa.MetaData()
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
        cls.local.connection.begin()

    @classmethod
    def commit(cls):
        cls.local.connection.commit()

    @classmethod
    def rollback(cls):
        cls.local.connection.rollback()

    @memoized_property
    def table(self):
        return self.__table__()

    @property
    def conn(self):
        return self.local.connection

    def __init__(self):
        self.table.metadata.bind = self.local.engine

    def exists(self, _id) -> bool:
        columns = [sa.func.count(self.table.c._id)]
        query = (
            sa.select(columns)
                .where(self.table.c._id == _id)
        )
        result = self.conn.execute(query)
        return bool(result.scalar())

    def query(
        self,
        predicate,
        fields=None,
        order_by=None,
        first=False,
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

    def fetch(self, _id, fields=None) -> Dict:
        records = self.fetch_many(_ids=[_id], fields=fields)
        return records[_id] if records else None

    def fetch_many(self, _ids: List, fields=None) -> Dict:
        # TODO: get field names from __schema__
        fields = set(fields or self.table.c.keys())
        fields.add('_id')
        columns = [
            getattr(self.table.c, k)
            for k in (fields or self.table.c.keys())
        ]
        query = sa.select(columns).where(self.table.c._id.in_(_ids))
        return {
            row._id: dict(row.items())
            for row in self.conn.execute(query).fetchmany()
        }

    def create(self, record: dict) -> dict:
        query = self.table.insert().values(**record)
        result = self.conn.execute(query)
        return self.fetch(_id=result.lastrowid)


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


    # Daos
    # ------------------------------------------------------------ 
    class UserDao(SqlalchemyExpressionLanguageDao):
        @staticmethod
        def __table__():
            return t_user

    class DogDao(SqlalchemyExpressionLanguageDao):
        @staticmethod
        def __table__():
            return t_dog


    # Schemas
    # ------------------------------------------------------------ 
    class UserSchema(Schema):
        name = fields.Str()

    class DogSchema(Schema):
        breed = fields.Str()
        owner_id = fields.Int(load_only=True)


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
        @staticmethod
        def __dao__():
            return DogDao

        @staticmethod
        def __schema__():
            return DogSchema


    # Test scenario
    # ------------------------------------------------------------ 
    SqlalchemyExpressionLanguageDao.initialize('sqlite://', meta=meta, echo=False)
    SqlalchemyExpressionLanguageDao.create_tables()

    SqlalchemyExpressionLanguageDao.connect()

    jeff = User(name='Jeff').save()
    chewy = Dog(breed='half', owner_id=jeff._id).save()
    poopy = Dog(breed='fecal', owner_id=jeff._id).save()

    user = User.get(_id=jeff._id, fields={'name', 'dogs'})
    print(json.dumps(user.dump(), indent=2, sort_keys=True))

    SqlalchemyExpressionLanguageDao.close()
