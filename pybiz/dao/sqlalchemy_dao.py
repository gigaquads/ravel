import sqlalchemy as sa

from copy import deepcopy
from typing import List, Dict, Text
from threading import local

from appyratus.decorators import memoized_property

from .base import Dao


class Join(object):
    def __init__(self, table, condition, fields, side='left'):
        self.table = table
        self.fields = fields
        self.side = side
        if not callable(condition):
            self.condition = lambda: condition
        else:
            self.condition = lambda: condition(self)

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
    def create_engine(cls, db_url):
        if cls.local.engine is not None:
            cls.local.engine.dispose()
        cls.local.engine = sa.create_engine(db_url)

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

    @memoized_property
    def joins(self):
        return self.__joins__()

    @memoized_property
    def field2join(self):
        field2join = {}
        for join in self.joins:
            field2join.update({k: join for k in join.fields})
        return field2join

    @property
    def conn(self):
        return self.local.connection

    def __init__(self):
        self.table.metadata.bind = self.local.engine

    def create_table(self):
        self.table.metadata.create_all(self.local.engine)

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

        fields = fields or self.table.c.keys()
        field2join = self._resolve_joins(fields)
        joins = set(field2join.values())

        # build list of columns to select, from driving and joined tables
        columns = self._get_columns(set(fields) - field2join.keys())
        for join in field2join.values():
            columns.extend(join.get_columns())

        # select row by _id
        query = sa.select(columns).where(self.table.c._id == _id)

        # add the joins
        for join in field2join.values():
            query = query.select_from(
                self.table.join(join.table, join.condition())
            )

        # return a list of dicts
        rows = self.conn.execute(query).fetchall()
        return self._rows_to_dict(rows)[0] if rows else None

    def create(self, record: dict) -> dict:
        query = (
            self.table
                .insert()
                .values(**record)
        )
        result = self.conn.execute(query)
        return self.fetch(_id=result.lastrowid)

    def _get_columns(self, column_names: List[Text]) -> List:
        return [getattr(self.table.c, k) for k in column_names]

    def _resolve_joins(self, column_names: List[Text]) -> Dict:
        return {
            k: self.field2join[k] for k in column_names
            if k in self.field2join
        }

    def _rows_to_dict(self, rows: List) -> List[Dict]:
        columns = self.table.c
        return [
            {
                c.name: getattr(row, c.name)
                for c in columns
            }
            for row in rows
        ]



t_cat = sa.Table('cat', sa.MetaData(), *[
    sa.Column('_id', sa.Integer(), primary_key=True),
    sa.Column('owner_user_id', sa.Integer(), index=True),
    sa.Column('color', sa.String())
])

t_user = sa.Table('user', sa.MetaData(), *[
    sa.Column('_id', sa.Integer(), primary_key=True),
    sa.Column('name', sa.String(), default='Barb')
])


class UserDao(SqlalchemyExpressionLanguageDao):

    @staticmethod
    def __table__():
        return t_user

    @staticmethod
    def __joins__():
        return [
            Join(
                table=t_cat,
                condition=t_cat.c.owner_user_id == t_user.c._id,
                fields={'color'}
            )
        ]


class CatDao(SqlalchemyExpressionLanguageDao):

    @staticmethod
    def __table__():
        return t_cat


SqlalchemyExpressionLanguageDao.create_engine('sqlite://')
SqlalchemyExpressionLanguageDao.connect()

cat_dao = CatDao()
cat_dao.create_table()

user_dao = UserDao()
user_dao.create_table()

print(user_dao.create({'_id': 1}))
print(user_dao.exists(1))
print(user_dao.fetch(_id=1, fields={'_id', 'name', 'color'}))

SqlalchemyExpressionLanguageDao.close()


'''
class SqlalchemyOrmDao(Dao):
    """
    """

    @staticmethod
    def __model__():
        raise NotImplementedError()

    @staticmethod
    def __schema__():
        raise NotImplementedError()

    @classmethod
    def connect():
        pass

    @property
    def db_session(self):
        pass

    @memoized_property
    def model_type(self):
        return self.__model__()

    @memoized_property
    def model_type(self):
        return self.__schema__()()

    def exists(self, _id) -> bool:
        return bool(
            self.db_session
                .query(self.model_type)
                .filter_by(_id=_id)
                .count()
            )

    def fetch(self, _id, fields=None) -> dict:
        fields = fields or self.schema.fields.keys()
        columns = [getattr(self.model_type, k) for k in fields]
        values = (
            self.db_session
                .query(*columns)
                .filter_by(_id=_id)
                .first()
            )
        return dict(zip(fields, values))

    def fetch_many(self, _ids, fields=None) -> list:
        fields = fields or self.schema.fields.keys()
        model_type = self.model_type
        columns = [getattr(model_type, k) for k in fields]
        values_list = (
            self.db_session
                .query(*columns)
                .filter(model_type._id.in_(_ids))
            )
        return [dict(zip(fields, values)) for values in values_list]

    def create(self, record: dict) -> dict:
        model = self.model_type()
        for k, v in record.items():
            setattr(model, k, v)
        self.db_session.begin_nested()
        self.db_session.add(model)
        self.db_session.commit()
        return {
            k: getattr(model, k) for k in self.schema.fields
        }

    def create_many(self, records: list) -> dict:
        raise NotImplementedError()

    def update(self, _id=None, data: dict=None) -> dict:
        raise NotImplementedError()

    def update_many(self, _ids: list, data: list=None) -> list:
        raise NotImplementedError()

    def delete(self, _id) -> dict:
        raise NotImplementedError()

    def delete_many(self, _ids: list) -> list:
        raise NotImplementedError()

    def query(self, predicate, **kwargs):
        raise NotImplementedError()

'''
