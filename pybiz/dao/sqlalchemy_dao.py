import sqlalchemy as sa

from copy import deepcopy
from typing import List, Dict, Text
from threading import local

from appyratus.decorators import memoized_property

from .base import Dao


class Join(object):
    def __init__(self, name: Text, table, conditions, side='left'):
        self.name = name
        self.table = table
        self.side = side
        if not callable(conditions):
            self.conditions = lambda: conditions
        else:
            self.conditions = lambda: conditions(self)

    def get_columns(self):
        return [getattr(self.table.c, k) for k in self.fields]


class SqlalchemyExpressionLanguageDao(object):

    local = local()
    local.engine = None
    local.connection = None

    @staticmethod
    def __table__():
        raise NotImplementedError()

    @staticmethod
    def __joins__() -> List[Join]:
        return []

    @classmethod
    def create_engine(cls, db_url, echo=False):
        if cls.local.engine is not None:
            cls.local.engine.dispose()
        cls.local.engine = sa.create_engine(db_url, echo=echo)

    @classmethod
    def connect(cls):
        cls.local.connection = cls.local.engine.connect()

    @classmethod
    def create_tables(cls, metadata):
        metadata.create_all(cls.local.engine)

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

    @memoized_property
    def joins(self):
        return {join.name: join for join in self.__joins__()}

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

    def fetch(self, _id, fields=None) -> dict:
        # TODO: get field names from __schema__
        # TODO: rename __joins__ to __relationships__

        joins = []
        columns = []
        sub_objects = {}
        for k in (fields or self.table.c.keys()):
            join = self.joins.get(k)
            if join:
                joins.append(join)
                columns.extend([
                    getattr(join.table.c, k).label('_{}_{}'.format(join.table.name, k))
                    for k in join.table.c.keys()
                ])
                sub_objects[join.table.name] = {}
            else:
                column = getattr(self.table.c, k)
                columns.append(column)

        # get row by id
        query = sa.select(columns).where(self.table.c._id == _id)

        # add the joins
        if joins:
            join_stmt = self.table
            for join in joins:
                for (join_table, join_cond) in join.conditions():
                    join_stmt = sa.outerjoin(join_stmt, join_table, join_cond)
            query = query.select_from(join_stmt)


        record = {}
        record.update(sub_objects)

        import re
        row = self.conn.execute(query).fetchone()
        for k, v in row.items():
            if k.startswith('_'):
                match = re.match(r'_(\w+)_', k)
                if match:
                    sub_objects[match.group()] = v
                    continue
            record[k] = v

        return record
        #rows = self.conn.execute(query).fetchall()
        #return self.to_dict(rows)[0] if rows else None

    def create(self, record: dict) -> dict:
        query = (
            self.table
                .insert()
                .values(**record)
        )
        result = self.conn.execute(query)
        return self.fetch(_id=result.lastrowid)

    def to_dict(self, rows: List) -> List[Dict]:
        columns = self.table.c
        return [
            {
                c.name: getattr(row, c.name)
                for c in columns
            }
            for row in rows
        ]


meta = sa.MetaData()

t_cat = sa.Table('cat', meta, *[
    sa.Column('_id', sa.Integer(), primary_key=True),
    sa.Column('owner_user_id', sa.Integer(), index=True),
    sa.Column('color', sa.String())
])

t_dog = sa.Table('dog', meta, *[
    sa.Column('_id', sa.Integer(), primary_key=True),
    sa.Column('breed', sa.String())
])

t_user = sa.Table('user', meta, *[
    sa.Column('_id', sa.Integer(), primary_key=True),
    sa.Column('name', sa.String(), default='Barb')
])

t_user_dog = sa.Table('user_dog', meta, *[
    sa.Column('user_id', sa.Integer(), primary_key=True),
    sa.Column('dog_id', sa.String(), primary_key=True)
])


class UserDao(SqlalchemyExpressionLanguageDao):

    @staticmethod
    def __table__():
        return t_user

    @staticmethod
    def __joins__():
        return [
            Join(
                name='cat',
                table=t_cat,
                conditions=[
                    (t_cat, t_cat.c.owner_user_id == t_user.c._id)
                ]
            ),
            Join(
                name='dogs',
                table=t_dog,
                conditions=[
                    (t_user_dog, t_user_dog.c.user_id == t_user.c._id),
                    (t_dog, t_dog.c._id == t_user_dog.c.dog_id),
                ]
            ),
        ]


class CatDao(SqlalchemyExpressionLanguageDao):

    @staticmethod
    def __table__():
        return t_cat


SqlalchemyExpressionLanguageDao.create_engine('sqlite://', echo=False)
SqlalchemyExpressionLanguageDao.create_tables(meta)
SqlalchemyExpressionLanguageDao.connect()

cat_dao = CatDao()
user_dao = UserDao()

print(user_dao.create({'_id': 1}))
print(user_dao.exists(1))
print(user_dao.fetch(_id=1, fields={'_id', 'name', 'dogs'}))

SqlalchemyExpressionLanguageDao.close()
