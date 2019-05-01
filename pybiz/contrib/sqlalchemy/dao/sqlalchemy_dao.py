import re
import threading
import uuid
import pickle

import sqlalchemy as sa

from typing import List, Dict, Text, Type, Set, Tuple

from appyratus.enum import EnumValueStr
from appyratus.env import Environment
from sqlalchemy.sql import bindparam

from pybiz.predicate import Predicate, ConditionalPredicate, BooleanPredicate
from pybiz.schema import fields, Field
from pybiz.util import JsonEncoder
from pybiz.dao.base import Dao

from .dialect import Dialect
from .sqlalchemy_table_builder import SqlalchemyTableBuilder


class SqlalchemyDao(Dao):

    local = threading.local()
    local.metadata = None

    json_encoder = JsonEncoder()

    env = Environment(
        SQLALCHEMY_ECHO=fields.Bool(default=lambda: False),
        SQLALCHEMY_URL=fields.String(default=lambda: 'sqlite://'),
        SQLALCHEMY_DIALECT=fields.Enum(
            fields.String(), Dialect.values(), default=Dialect.sqlite
        )
    )

    @classmethod
    def get_default_adapters(cls, dialect: Dialect) -> List[Field.TypeAdapter]:
        adapters = [
            fields.Field.adapt(
                on_adapt=lambda field: sa.Text,
                on_encode=lambda x: cls.json_encoder.encode(x),
                on_decode=lambda x: cls.json_encoder.decode(x),
            ),
            fields.Float.adapt(on_adapt=lambda field: sa.Float),
            fields.Bool.adapt(on_adapt=lambda field: sa.Boolean),
            fields.DateTime.adapt(on_adapt=lambda field: sa.DateTime),
            fields.Timestamp.adapt(on_adapt=lambda field: sa.DateTime),
            fields.Enum.adapt(on_adapt=lambda field: {
                    fields.String: sa.Text,
                    fields.Int: sa.Integer,
                }[type(field.nested)]
            ),
        ]
        adapters.extend(
            field_type.adapt(on_adapt=lambda field: sa.Text)
            for field_type in {
                fields.String, fields.FormatString,
                fields.UuidString, fields.DateTimeString
            }
        )
        adapters.extend(
            field_type.adapt(on_adapt=lambda field: sa.BigInteger)
            for field_type in {
                fields.Int, fields.Uint32, fields.Uint64,
                fields.Sint32, fields.Sint64
            }
        )
        if dialect == Dialect.postgresql:
            adapters.extend(cls.get_postgresql_default_adapters())
        elif dialect == Dialect.mysql:
            adapters.extend(cls.get_mysql_default_adapters())
        elif dialect == Dialect.sqlite:
            adapters.extend(cls.get_sqlite_default_adapters())

        return adapters

    @classmethod
    def get_postgresql_default_adapters(cls) -> List[Field.TypeAdapter]:
        pg_types = sa.dialect.postgresql
        return [
            fields.Uuid.adapt(on_adapt=lambda field: pg_types.UUID),
            fields.Dict.adapt(on_adapt=lambda field: pg_types.JSONB),
            fields.Nested.adapt(on_adapt=lambda field: pg_types.JSONB),
            fields.Set.adapt(
                on_adapt=lambda field: pg_types.JSONB,
                on_encode=lambda x: list(x),
                on_decode=lambda x: set(x)
            ),
            fields.List.adapt(on_adapt=lambda field: ARRAY({
                    fields.String: sa.Text,
                    fields.Int: sa.Integer,
                    fields.Bool: sa.Boolean,
                    fields.Float: sa.Float,
                    fields.DateTime: sa.DateTime,
                    fields.Timestamp: sa.DateTime,
                    fields.Dict: pg_types.JSONB,
                    fields.Nested: pg_types.JSONB,
                }[type(field.nested)])
            ),
        ]

    @classmethod
    def get_mysql_default_adapters(cls) -> List[Field.TypeAdapter]:
        return [
            fields.Dict.adapt(on_adapt=lambda field: sa.JSON),
            fields.Nested.adapt(on_adapt=lambda field: sa.JSON),
            fields.List.adapt(on_adapt=lambda field: sa.JSON),
            fields.Set.adapt(
                on_adapt=lambda field: sa.JSON,
                on_encode=lambda x: cls.json_encoder.encode(x),
                on_decode=lambda x: set(cls.json_encoder.decode(x))
            ),
        ]

    @classmethod
    def get_sqlite_default_adapters(cls) -> List[Field.TypeAdapter]:
        adapters = [
            field_type.adapt(
                on_adapt=lambda field: sa.Text,
                on_encode=lambda x: cls.json_encoder.encode(x),
                on_decode=lambda x: cls.json_encoder.decode(x),
            )
            for field_type in {
                fields.Dict, fields.List, fields.Nested
            }
        ]
        adapters.append(
            fields.Set.adapt(
                on_adapt=lambda field: sa.Text,
                on_encode=lambda x: cls.json_encoder.encode(x),
                on_decode=lambda x: set(cls.json_encoder.decode(x))
            )
        )
        return adapters

    def __init__(self, adapters: List[Field.TypeAdapter] = None):
        super().__init__()
        self._custom_adapters = adapters or []
        self._table = None
        self._builder = None
        self._adapters = None

    @property
    def adapters(self):
        return self._adapters

    def adapt_record(self, record: Dict, serialize=True) -> Dict:
        cb_name = 'on_encode' if serialize else 'on_decode'
        prepared_record = {}
        for k, v in record.items():
            if k in ('_rev'):
                prepared_record[k] = v
            adapter = self._adapters.get(k)
            if adapter:
                callback = getattr(adapter, cb_name)
                if callback:
                    prepared_record[k] = callback(v)
                    continue
            prepared_record[k] = v
        return prepared_record

    def adapt_id(self, _id, serialize=True):
        cb_name = 'on_encode' if serialize else 'on_decode'
        adapter = self._adapters.get('_id')
        if adapter:
            callback = getattr(adapter, cb_name)
            if callback:
                return callback(_id)
        return _id

    @classmethod
    def on_bootstrap(cls, url=None, dialect=None, echo=False):
        url = url or self._url or cls.env.SQLALCHEMY_URL
        cls.dialect = dialect or cls.env.SQLALCHEMY_DIALECT
        cls.local.metadata = sa.MetaData()
        cls.local.metadata.bind = sa.create_engine(
            name_or_url=url,
            echo=bool(echo or cls.env.SQLALCHEMY_ECHO),
        )

    def on_bind(self, biz_type: Type['BizObject'], **kwargs):
        field_type_2_adapter = {
            adapter.field_type: adapter for adapter in
            self.get_default_adapters(self.dialect) + self._custom_adapters
        }
        self._adapters = {
            field_name: field_type_2_adapter[type(field)]
            for field_name, field in self.biz_type.schema.fields.items()
            if type(field) in field_type_2_adapter
        }
        self._builder = SqlalchemyTableBuilder(self)
        self._table = self._builder.build_table()

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
            page = [
                self.adapt_record(dict(row.items()), serialize=False)
                for row in cursor.fetchmany(512)
            ]
            if page:
                records.extend(page)
            else:
                break

        return records

    def _prepare_predicate(self, pred, empty=set()):
        if isinstance(pred, ConditionalPredicate):
            adapter = self._adapters.get(pred.field.source)
            if adapter and adapter.on_encode:
                pred.value = adapter.on_encode(pred.value)
            col = getattr(self.table.c, pred.field.source)
            if pred.op == '=':
                return col == pred.value
            elif pred.op == '!=':
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
                .where(self.table.c._id == self.adapt_id(_id))
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

    def fetch_many(self, _ids: List, fields=None, as_list=False) -> Dict:
        prepared_ids = [self.adapt_id(_id, serialize=True) for _id in _ids]
        fields = set(fields or self.biz_type.schema.fields.keys())
        fields.update(['_id', '_rev'])
        columns = [getattr(self.table.c, k) for k in fields]
        select_stmt = sa.select(columns)
        if prepared_ids:
            select_stmt = select_stmt.where(self.table.c._id.in_(prepared_ids))
        cursor = self.conn.execute(select_stmt)
        records = {} if not as_list else []

        while True:
            page = cursor.fetchmany(512)
            if page:
                for row in page:
                    raw_record = dict(row.items())
                    record = self.adapt_record(raw_record, serialize=False)
                    _id = self.adapt_id(row._id, serialize=False)
                    if as_list:
                        records.append(record)
                    else:
                        records[_id] = record
            else:
                break

        return records

    def fetch_all(self, fields: Set[Text] = None) -> Dict:
        return self.fetch_many([], fields=fields)

    def create(self, record: dict) -> dict:
        record['_id'] = self.create_id(record)
        prepared_record = self.adapt_record(record, serialize=True)
        insert_stmt = self.table.insert().values(**prepared_record)
        if self.supports_returning:
            insert_stmt = insert_stmt.return_defaults()
            result = self.conn.execute(insert_stmt)
            return dict(record, **(result.returned_defaults or {}))
        else:
            result = self.conn.execute(insert_stmt)
            return self.fetch(_id=record['_id'])

    def create_many(self, records: List[Dict]) -> Dict:
        prepared_records = []
        for record in records:
            record['_id'] = self.create_id(record)
            prepared_record = self.adapt_record(record, serialize=True)
            prepared_records.append(prepared_record)

        self.conn.execute(self.table.insert(), prepared_records)
        if self.supports_returning:
            # TODO: use implicit returning if possible
            pass
        else:
            return self.fetch_many(
                (rec['_id'] for rec in records), as_list=True)

    def update(self, _id, data: Dict) -> Dict:
        prepared_id = self.adapt_id(_id)
        prepared_data = self.adapt_record(data, serialize=True)
        update_stmt = (
            self.table
                .update()
                .values(**prepared_data)
                .where(self.table.c._id == prepared_id)
            )
        if self.supports_returning:
            update_stmt = update_stmt.return_defaults()
            result = self.conn.execute(update_stmt)
            return dict(data, **(result.returned_defaults or {}))
        else:
            self.conn.execute(update_stmt)
            return self.fetch(_id)

    def update_many(self, _ids: List, data: Dict = None) -> None:
        assert data

        prepared_ids = [self.adapt_id(_id) for _id in _ids]
        prepared_data = [
            self.adapt_record(record, serialize=True)
            for record in data
        ]
        values = {
            k: bindparam(k) for k in prepared_data[0].keys()
        }
        update_stmt = (
            self.table
                .update()
                .where(self.table.c._id == bindparam('_id'))
                .values(**values)
        )
        self.conn.execute(update_stmt, prepared_data)

        if self.supports_returning:
            # TODO: use implicit returning if possible
            return self.fetch_many(_ids, as_list=True)
        else:
            return self.fetch_many(_ids, as_list=True)

    def delete(self, _id) -> None:
        prepared_id = self.adapt_id(_id)
        delete_stmt = self.table.delete().where(
            self.table.c._id == prepared_id
        )
        self.conn.execute(delete_stmt)

    def delete_many(self, _ids: list) -> None:
        prepared_ids = [self.adapt_id(_id) for _id in _ids]
        delete_stmt = self.table.delete().where(
            self.table.c._id.in_(prepared_ids)
        )
        self.conn.execute(delete_stmt)

    def delete_all(self):
        delete_stmt = self.table.delete()
        self.conn.execute(delete_stmt)

    @property
    def table(self):
        return self._table

    @property
    def conn(self):
        return self.local.connection

    @property
    def supports_returning(self):
        if not self.is_bootstrapped():
            return False
        return self.local.metadata.bind.dialect.implicit_returning

    @classmethod
    def create_tables(cls):
        if not cls.is_bootstrapped():
            return

        meta = cls.get_metadata()
        engine = cls.get_engine()

        # create all tables
        meta.create_all(engine)

        # add a trigger to each table to auto-increment
        # the _rev column on update.
        for table in meta.tables.values():
            engine.execute(f'''
                create trigger incr_{table.name}_rev_on_update
                after update on {table.name}
                for each row
                begin
                    update {table.name} set _rev = old._rev + 1
                    where _id = old._id;
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

    @classmethod
    def get_engine(cls):
        return cls.get_metadata().bind
