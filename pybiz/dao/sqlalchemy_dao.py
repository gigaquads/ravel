import re
import threading
import uuid

import ujson

import sqlalchemy as sa

from copy import deepcopy
from typing import List, Dict, Text, Type, Set, Tuple

from appyratus.memoize import memoized_property
from appyratus.utils import StringUtils
from appyratus.enum import EnumValueStr
from appyratus.env import Environment

from pybiz.api.middleware import RegistryMiddleware
from pybiz.predicate import Predicate, ConditionalPredicate, BooleanPredicate
from pybiz.schema import fields, Field
from pybiz.util import JsonEncoder

from .base import Dao


class Dialect(EnumValueStr):
    @staticmethod
    def values():
        return {'postgresql', 'mysql', 'sqlite'}


class ColumnAdapter(object):
    json_encoder = JsonEncoder()

    def __init__(
        self,
        source: Type[Field],
        on_define: Type[sa.Column],
        on_serialize=None,
        on_deserialize=None,
    ):
        self.source = source
        self.on_define = on_define
        self.on_serialize = on_serialize
        self.on_deserialize = on_deserialize

    def serialize(self, value):
        if self.on_serialize is not None:
            return self.on_serialize(value)
        else:
            return value

    def deserialize(self, value):
        if self.on_deserialize is not None:
            return self.on_deserialize(value)
        else:
            return value

    @classmethod
    def defaults(cls, dialect: Dialect) -> List['ColumnAdapter']:
        adapters = [
            cls(
                source=fields.Field,
                on_define=lambda field: sa.Text,
                on_serialize=lambda x: str(x)
            ),
            cls(
                source=fields.String,
                on_define=lambda field: sa.Text
            ),
            cls(
                source=fields.Email,
                on_define=lambda field: sa.Text
            ),
            cls(
                source=fields.FormatString,
                on_define=lambda field: sa.Text
            ),
            cls(
                source=fields.UuidString,
                on_define=lambda field: sa.Text
            ),
            cls(
                source=fields.DateTimeString,
                on_define=lambda field: sa.Text
            ),
            cls(
                source=fields.Int,
                on_define=lambda field: sa.BigInteger
            ),
            cls(
                source=fields.Uint32,
                on_define=lambda field: sa.Integer
            ),
            cls(
                source=fields.Uint64,
                on_define=lambda field: sa.Integer
            ),
            cls(
                source=fields.Sint32,
                on_define=lambda field: sa.Integer
            ),
            cls(
                source=fields.Sint64,
                on_define=lambda field: sa.Integer
            ),
            cls(
                source=fields.Float,
                on_define=lambda field: sa.Float
            ),
            cls(
                source=fields.Bool,
                on_define=lambda field: sa.Boolean
            ),
            cls(
                source=fields.DateTime,
                on_define=lambda field: sa.DateTime
            ),
            cls(
                source=fields.Timestamp,
                on_define=lambda field: sa.DateTime
            ),
            cls(
                source=fields.Enum,
                on_define=(
                    lambda field: {
                        fields.String: sa.Text,
                        fields.Int: sa.Integer,
                    }[type(field.nested)]
                ),
            ),
        ]
        if dialect == Dialect.postgresql:
            adapters.extend([
                cls(
                    source=fields.Uuid,
                    on_define=lambda field: sa.dialects.postgresql.UUID
                ),
                cls(
                    source=fields.Dict,
                    on_define=lambda field: sa.dialects.postgresql.JSONB
                ),
                cls(
                    source=fields.Nested,
                    on_define=lambda field: sa.dialects.postgresql.JSONB
                ),
                cls(
                    source=fields.List,
                    coluumn_type=(
                        lambda field: sa.dialects.postgresql.ARRAY({
                            fields.String: sa.Text,
                            fields.Int: sa.Integer,
                            fields.Bool: sa.Boolean,
                            fields.Float: sa.Float,
                            fields.DateTime: sa.DateTime,
                            fields.Timestamp: sa.DateTime,
                            fields.Dict: sa.dialects.postgresql.JSONB,
                            fields.Nested: sa.dialects.postgresql.JSONB,
                        }[type(field.nested)])
                    )
                ),
                cls(
                    source=fields.Set,
                    on_define=lambda field: sa.dialects.postgresql.JSONB,
                    on_serialize=lambda x: list(x),
                    on_deserialize=lambda x: set(x)
                )
            ])
        elif dialect == Dialect.mysql:
            adapters.extend([
                cls(
                    source=fields.Dict,
                    on_define=lambda field: sa.JSON
                ),
                cls(
                    source=fields.Nested,
                    on_define=lambda field: sa.JSON
                ),
                cls(
                    source=fields.List,
                    on_define=lambda field: sa.JSON
                ),
                cls(
                    fields.Set, sa.JSON,
                    on_serialize=lambda x: cls.json_encoder.encode(x),
                    on_deserialize=lambda x: set(x)
                ),
            ])
        else:
            adapters.extend([
                cls(
                    source=fields.Dict,
                    on_define=lambda field: sa.Text,
                    on_serialize=lambda x: cls.json_encoder.encode(x),
                    on_deserialize=lambda x: ujson.loads(x)
                ),
                cls(
                    source=fields.List,
                    on_define=lambda field: sa.Text,
                    on_serialize=lambda x: cls.json_encoder.encode(x),
                    on_deserialize=lambda x: ujson.loads(x)
                ),
                cls(
                    source=fields.Set,
                    on_define=sa.Text,
                    on_serialize=lambda x: cls.json_encoder.encode(x),
                    on_deserialize=lambda x: set(ujson.loads(x))
                ),
            ])

        return adapters


class TableBuilder(object):
    def __init__(
        self,
        dao: 'SqlalchemyDao',
        adapters: List[ColumnAdapter] = None
    ):
        self._dialect = dao.dialect
        self._biz_type = dao.biz_type
        self._metadata = dao.get_metadata()
        self._adapters = {
            adapter.source: adapter for adapter in
            ColumnAdapter.defaults(self._dialect) + (adapters or [])
        }

    @property
    def adapters(self):
        return self._adapters

    @property
    def dialect(self):
        return self._dialect

    def build_table(self) -> sa.Table:
        columns = [
            self.build_column(field)
            for field in self._biz_type.schema.fields.values()
        ]
        table_name = StringUtils.snake(self._biz_type.__name__)
        table = sa.Table(table_name, self._metadata, *columns)
        return table

    def build_column(self, field: Field) -> sa.Column:
        name = field.source
        dtype = self.adapters.get(type(field)).on_define(field)
        primary_key = field.name == '_id'

        meta = field.meta.get('sa', {})
        indexed = (field.name in ['_rev']) or meta.get('indexed', False)
        unique = meta.get('unique', False)

        server_default = None
        if field.source == '_rev':
            server_default = '0'

        return sa.Column(
            name,
            dtype(),
            index=indexed,
            primary_key=primary_key,
            nullable=field.nullable,
            unique=unique,
            server_default=server_default
        )


class SqlalchemyDaoMiddleware(RegistryMiddleware):
    def pre_request(self, proxy, args: Tuple, kwargs: Dict):
        """
        In pre_request, args and kwargs are in the raw form before being
        processed by registry.on_request.
        """
        SqlalchemyDao.connect()
        SqlalchemyDao.begin()

    def post_request(self, proxy, args: Tuple, kwargs: Dict, result):
        """
        In post_request, args and kwargs are in the form output by
        registry.on_request.
        """
        # TODO: pass in exc to post_request if there
        #   was an exception and rollback
        try:
            SqlalchemyDao.commit()
        except:
            SqlalchemyDao.rollback()
        finally:
            SqlalchemyDao.close()


class SqlalchemyDao(Dao):
    Middleware = SqlalchemyDaoMiddleware

    local = threading.local()
    local.metadata = None

    params = None
    dialect = None

    env = Environment(
        SQLALCHEMY_ECHO=fields.Bool(required=True, default=lambda: False),
        SQLALCHEMY_URL=fields.String(required=True, default='sqlite://'),
        SQLALCHEMY_DIALECT=fields.Enum(
            fields.String(), list(Dialect.values()), default=Dialect.sqlite
        )
    )

    def __init__(self):
        super().__init__()
        self._table = None

    @classmethod
    def bootstrap(cls, registry: 'Registry' = None, **kwargs):
        super().bootstrap(registry)
        cls.dialect = kwargs.get('dialect') or cls.env.SQLALCHEMY_DIALECT
        cls.local.metadata = sa.MetaData()
        cls.local.metadata.bind = sa.create_engine(
            name_or_url=kwargs.get('url') or cls.env.SQLALCHEMY_URL,
            echo=bool(kwargs.get('echo', cls.env.SQLALCHEMY_ECHO)),
        )

    def bind(self, biz_type: Type['BizObject']):
        super().bind(biz_type)
        self._table = TableBuilder(dao=self).build_table()

    def create_id(self, record: Dict) -> object:
        return record.get('_id', uuid.uuid4().hex)

    def query(
        self,
        predicate,
        fields=None,
        limit=None,
        offset=None,
        order_by=None,  # TODO: implement order_by
        **kwargs,
    ):
        fields = fields or self.biz_type.schema.fields.keys()
        fields.update(['_id', '_rev'])

        columns = [getattr(self.table.c, k) for k in fields]
        predicate = Predicate.deserialize(predicate)
        filters = self._prepare_predicate(predicate)

        # build the query object
        query = sa.select(columns).where(filters)
        if limit is not None:
            query = query.limit(max(0, limit))
        if offset is not None:
            query = query.offset(max(0, limit))

        # execute query, aggregating resulting records
        cursor = self.conn.execute(query)
        records = []

        while True:
            page = [dict(row.items()) for row in cursor.fetchmany(512)]
            if page:
                records.extend(page)
            else:
                break

        return records

    def _prepare_predicate(self, pred, empty=set()):
        if isinstance(pred, ConditionalPredicate):
            col = getattr(self.table.c, pred.field.source)
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
                lhs_result = self._prepare_predicate(pred.lhs)
                rhs_result = self._prepare_predicate(pred.rhs)
                return sa.and_(lhs_result, rhs_result)
            elif pred.op == '|':
                lhs_result = self._prepare_predicate(pred.lhs)
                rhs_result = self._prepare_predicate(pred.rhs)
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

    def count(self) -> int:
        query = sa.select([sa.func.count(self.table.c._id)])
        result = self.conn.execute(query)
        return result.scalar()

    def fetch(self, _id, fields=None) -> Dict:
        records = self.fetch_many(_ids=[_id], fields=fields)
        return records[_id] if records else None

    def fetch_many(self, _ids: List, fields=None) -> Dict:
        fields = set(fields or self.biz_type.schema.fields.keys())
        fields.update(['_id', '_rev'])
        columns = [getattr(self.table.c, k) for k in fields]
        select_stmt = sa.select(columns)
        if _ids:
            select_stmt = select_stmt.where(self.table.c._id.in_(_ids))
        cursor = self.conn.execute(select_stmt)
        records = {}
        while True:
            page = cursor.fetchmany(512)
            if page:
                for row in page:
                    records[row._id] = dict(row.items())
            else:
                return records

    def fetch_all(self, fields: Set[Text] = None) -> Dict:
        return self.fetch_many([], fields=fields)

    def create(self, record: dict) -> dict:
        record['_id'] = self.create_id(record)
        insert_stmt = self.table.insert().values(**record)

        if self.supports_returning:
            insert_stmt = insert_stmt.return_defaults()
            result = self.conn.execute(insert_stmt)
            return dict(record, **(result.returned_defaults or {}))
        else:
            result = self.conn.execute(insert_stmt)
            return self.fetch(_id=record['_id'])

    def create_many(self, records: List[Dict]) -> Dict:
        for record in records:
            record['_id'] = self.create_id(record)
        self.conn.execute(self.table.insert(), records)
        # TODO: return something

    def update(self, _id, data: Dict) -> Dict:
        assert data
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
            # TODO: ensure _rev comes back too in defaults
        else:
            self.conn.execute(update_stmt)
            return self.fetch(_id)

    def update_many(self, _ids: List, data: Dict = None) -> None:
        assert data
        update_stmt = (
            self.table
                .update()
                .values(**data)
                .where(self.table.c._id.in_(_ids))
            )
        self.conn.execute(update_stmt)
        # TODO: return updated

    def delete(self, _id) -> None:
        delete_stmt = self.table.delete().where(self.table.c._id == _id)
        self.conn.execute(delete_stmt)

    def delete_many(self, _ids: list) -> None:
        delete_stmt = self.table.delete().where(self.table.c._id.in_(_ids))
        self.conn.execute(delete_stmt)

    @property
    def table(self):
        return self._table

    @property
    def conn(self):
        return self.local.connection

    @property
    def supports_returning(self):
        return self.local.metadata.bind.dialect.implicit_returning

    @classmethod
    def create_tables(cls):
        engine = cls.get_metadata().bind
        cls.get_metadata().create_all(engine)

        # add a trigger to each table to auto-increment
        # the _rev column on update.
        for table in cls.get_metadata().tables.values():
            engine.execute(f'''
                create trigger incr_{table.name}_rev_on_update
                after update on {table.name}
                for each row
                begin
                    update {table.name} set _rev = old._rev + 1
                    where _id = new._id;
                end
            ''')

    @classmethod
    def connect(cls):
        cls.local.connection = cls.local.metadata.bind.connect()

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

    @classmethod
    def get_metadata(cls):
        return cls.local.metadata
