import re
import threading
import uuid
import pickle

import sqlalchemy as sa

from typing import List, Dict, Text, Type, Set, Tuple

from sqlalchemy import TypeDecorator
from sqlalchemy.dialects.postgresql import ARRAY

from appyratus.enum import EnumValueStr
from appyratus.env import Environment
from sqlalchemy.sql import bindparam
from sqlalchemy.dialects.postgresql import ARRAY

from pybiz.predicate import (
    Predicate, ConditionalPredicate, BooleanPredicate,
    OP_CODE,
)
from pybiz.schema import fields, Field
from pybiz.util.json_encoder import JsonEncoder
from pybiz.util.loggers import console
from pybiz.store.base import Store
from pybiz.constants import REV_FIELD_NAME, ID_FIELD_NAME

from .dialect import Dialect
from .sqlalchemy_table_builder import SqlalchemyTableBuilder
from ..types import ArrayOfEnum


class SqlalchemyStore(Store):

    local = threading.local()
    local.metadata = None
    local.connection = None

    json_encoder = JsonEncoder()
    id_column_names = {}

    env = Environment(
        SQLALCHEMY_DAO_ECHO=fields.Bool(default=False),
        SQLALCHEMY_DAO_DIALECT=fields.Enum(
            fields.String(), Dialect.values(), default=Dialect.sqlite
        ),
        SQLALCHEMY_DAO_PROTOCOL=fields.String(default='sqlite'),
        SQLALCHEMY_DAO_USER=fields.String(default='postgres'),
        SQLALCHEMY_DAO_HOST=fields.String(default='0.0.0.0'),
        SQLALCHEMY_DAO_PORT=fields.Int(default=5432),
        SQLALCHEMY_DAO_NAME=fields.String(),
    )

    @classmethod
    def get_default_adapters(cls, dialect: Dialect) -> List[Field.Adapter]:
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
            field_class.adapt(on_adapt=lambda field: sa.Text)
            for field_class in {
                fields.String, fields.FormatString,
                fields.UuidString, fields.DateTimeString
            }
        )
        adapters.extend(
            field_class.adapt(on_adapt=lambda field: sa.BigInteger)
            for field_class in {
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
    def get_postgresql_default_adapters(cls) -> List[Field.Adapter]:
        pg_types = sa.dialects.postgresql

        def on_adapt_list(field):
            if isinstance(field.nested, fields.Enum):
                return ArrayOfEnum(
                    pg_types.ENUM(*field.nested.values)
                )
            return pg_types.ARRAY({
                fields.String: sa.Text,
                fields.Uuid: pg_types.UUID,
                fields.Int: sa.Integer,
                fields.Bool: sa.Boolean,
                fields.Float: sa.Float,
                fields.DateTime: sa.DateTime,
                fields.Timestamp: sa.DateTime,
                fields.Dict: pg_types.JSONB,
                fields.Field: pg_types.JSONB,
                fields.Nested: pg_types.JSONB,
            }.get(type(field.nested), sa.Text))

        return [
            fields.Field.adapt(on_adapt=lambda field: pg_types.JSONB),
            fields.Uuid.adapt(on_adapt=lambda field: pg_types.UUID),
            fields.Dict.adapt(on_adapt=lambda field: pg_types.JSONB),
            fields.Nested.adapt(on_adapt=lambda field: pg_types.JSONB),
            fields.Set.adapt(
                on_adapt=lambda field: pg_types.JSONB,
                on_encode=lambda x: list(x),
                on_decode=lambda x: set(x)
            ),
            fields.UuidString.adapt(
                on_adapt=lambda field: pg_types.UUID,
                on_decode=lambda x: x.replace('-', '') if x else x,
            ),
            fields.List.adapt(on_adapt=on_adapt_list)
        ]

    @classmethod
    def get_mysql_default_adapters(cls) -> List[Field.Adapter]:
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
    def get_sqlite_default_adapters(cls) -> List[Field.Adapter]:
        adapters = [
            field_class.adapt(
                on_adapt=lambda field: sa.Text,
                on_encode=lambda x: cls.json_encoder.encode(x),
                on_decode=lambda x: cls.json_encoder.decode(x),
            )
            for field_class in {
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

    def __init__(self, adapters: List[Field.Adapter] = None):
        super().__init__()
        self._custom_adapters = adapters or []
        self._table = None
        self._builder = None
        self._adapters = None
        self._id_column = None

    @property
    def adapters(self):
        return self._adapters

    @property
    def id_column_name(self):
        return self.biz_class.Schema.fields[ID_FIELD_NAME].source

    def adapt_record(self, record: Dict, serialize=True) -> Dict:
        cb_name = 'on_encode' if serialize else 'on_decode'
        prepared_record = {}
        for k, v in record.items():
            if k in (REV_FIELD_NAME):
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
        adapter = self._adapters.get(self.id_column_name)
        if adapter:
            callback = getattr(adapter, cb_name)
            if callback:
                return callback(_id)
        return _id

    @classmethod
    def on_bootstrap(cls, url=None, dialect=None, echo=False):
        url = url or (
            '{protocol}://{user}@{host}:{port}/{db}'.format(
                protocol=cls.env.SQLALCHEMY_DAO_PROTOCOL,
                user=cls.env.SQLALCHEMY_DAO_USER,
                host=cls.env.SQLALCHEMY_DAO_HOST,
                port=cls.env.SQLALCHEMY_DAO_PORT,
                db=cls.env.SQLALCHEMY_DAO_NAME,
            )
        )
        cls.dialect = dialect or cls.env.SQLALCHEMY_DAO_DIALECT
        cls.local.metadata = sa.MetaData()

        console.debug(
            message=f'creating Sqlalchemy engine',
            data={
                'echo': bool(cls.env.SQLALCHEMY_DAO_ECHO),
                'url': url,
                'dialect': cls.dialect,
            }
        )

        cls.local.metadata.bind = sa.create_engine(
            name_or_url=url,
            echo=bool(echo or cls.env.SQLALCHEMY_DAO_ECHO),
        )

    def on_bind(
        self,
        biz_class: Type['Resource'],
        table: Text = None,
        schema: 'Schema' = None,
        **kwargs
    ):
        field_class_2_adapter = {
            adapter.field_class: adapter for adapter in
            self.get_default_adapters(self.dialect) + self._custom_adapters
        }
        self._adapters = {
            field_name: field_class_2_adapter[type(field)]
            for field_name, field in self.biz_class.Schema.fields.items()
            if type(field) in field_class_2_adapter
        }
        self._builder = SqlalchemyTableBuilder(self)
        self._table = self._builder.build_table(name=table, schema=schema)
        self._id_column = getattr(self._table.c, self.id_column_name)

        self.id_column_names[self._table.name] = self.id_column_name

    def query(
        self,
        predicate: 'Predicate',
        fields: Set[Text] = None,
        limit: int = None,
        offset: int = None,
        order_by: Tuple = None,
        **kwargs,
    ):
        fields = fields or {k: None for k in self.biz_class.Schema.fields}
        fields.update({
            self.id_column_name: None,
            self.biz_class.Schema.fields[REV_FIELD_NAME].source: None,
        })

        columns = [getattr(self.table.c, k) for k in fields]
        predicate = Predicate.deserialize(predicate)
        filters = self._prepare_predicate(predicate)

        # build the query object
        query = sa.select(columns).where(filters)

        if order_by:
            sa_order_by = [
                sa.desc(getattr(self.table.c, x.key)) if x.desc else
                sa.asc(getattr(self.table.c, x.key))
                for x in order_by
            ]
            query = query.order_by(*sa_order_by)

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
            if pred.op == OP_CODE.EQ:
                return col == pred.value
            elif pred.op == OP_CODE.NEQ:
                return col != pred.value
            if pred.op == OP_CODE.GEQ:
                return col >= pred.value
            elif pred.op == OP_CODE.GT:
                return col > pred.value
            elif pred.op == OP_CODE.LT:
                return col < pred.value
            elif pred.op == OP_CODE.LEQ:
                return col <= pred.value
            elif pred.op == OP_CODE.INCLUDING:
                return col.in_(pred.value)
            elif pred.op == OP_CODE.EXCLUDING:
                return ~col.in_(pred.value)
            else:
                raise Exception('unrecognized conditional predicate')
        elif isinstance(pred, BooleanPredicate):
            if pred.op == OP_CODE.AND:
                lhs_result = self._prepare_predicate(pred.lhs)
                rhs_result = self._prepare_predicate(pred.rhs)
                return sa.and_(lhs_result, rhs_result)
            elif pred.op == OP_CODE.OR:
                lhs_result = self._prepare_predicate(pred.lhs)
                rhs_result = self._prepare_predicate(pred.rhs)
                return sa.or_(lhs_result, rhs_result)
            else:
                raise Exception('unrecognized boolean predicate')
        else:
            raise Exception('unrecognized predicate type')

    def exists(self, _id) -> bool:
        columns = [sa.func.count(self._id_column)]
        query = (
            sa.select(columns)
                .where(self._id_column == self.adapt_id(_id))
        )
        result = self.conn.execute(query)
        return bool(result.scalar())

    def count(self) -> int:
        query = sa.select([sa.func.count(self._id_column)])
        result = self.conn.execute(query)
        return result.scalar()

    def fetch(self, _id, fields=None) -> Dict:
        records = self.fetch_many(_ids=[_id], fields=fields)
        return records[_id] if records else None

    def fetch_many(self, _ids: List, fields=None, as_list=False) -> Dict:
        prepared_ids = [self.adapt_id(_id, serialize=True) for _id in _ids]

        if fields:
            if not isinstance(fields, set):
                fields = set(fields)
        else:
            fields = {
                f.source for f in self.biz_class.Schema.fields.values()
            }
        fields.update({
            self.id_column_name,
            self.biz_class.Schema.fields[REV_FIELD_NAME].source,
        })

        columns = [getattr(self.table.c, k) for k in fields]
        select_stmt = sa.select(columns)

        id_col = getattr(self.table.c, self.id_column_name)

        if prepared_ids:
            select_stmt = select_stmt.where(id_col.in_(prepared_ids))
        cursor = self.conn.execute(select_stmt)
        records = {} if not as_list else []

        while True:
            page = cursor.fetchmany(512)
            if page:
                for row in page:
                    raw_record = dict(row.items())
                    record = self.adapt_record(raw_record, serialize=False)
                    _id = self.adapt_id(
                        row[self.id_column_name], serialize=False
                    )
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
        record[self.id_column_name] = self.create_id(record)
        prepared_record = self.adapt_record(record, serialize=True)
        insert_stmt = self.table.insert().values(**prepared_record)
        if self.supports_returning:
            insert_stmt = insert_stmt.return_defaults()
            result = self.conn.execute(insert_stmt)
            return dict(record, **(result.returned_defaults or {}))
        else:
            result = self.conn.execute(insert_stmt)
            return self.fetch(_id=record[self.id_column_name])

    def create_many(self, records: List[Dict]) -> Dict:
        prepared_records = []
        for record in records:
            record[self.id_column_name] = self.create_id(record)
            prepared_record = self.adapt_record(record, serialize=True)
            prepared_records.append(prepared_record)

        self.conn.execute(self.table.insert(), prepared_records)
        if self.supports_returning:
            # TODO: use implicit returning if possible
            pass

        return self.fetch_many(
            (rec[self.id_column_name] for rec in records), as_list=True)

    def update(self, _id, data: Dict) -> Dict:
        prepared_id = self.adapt_id(_id)
        prepared_data = self.adapt_record(data, serialize=True)
        update_stmt = (
            self.table
                .update()
                .values(**prepared_data)
                .where(self._id_column == prepared_id)
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
                .where(self._id_column == bindparam(self.id_column_name))
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
            self._id_column == prepared_id
        )
        self.conn.execute(delete_stmt)

    def delete_many(self, _ids: list) -> None:
        prepared_ids = [self.adapt_id(_id) for _id in _ids]
        delete_stmt = self.table.delete().where(
            self._id_column.in_(prepared_ids)
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
        if self.local.connection is None:
            self.connect()
        return self.local.connection

    @property
    def supports_returning(self):
        if not self.is_bootstrapped():
            return False
        return self.local.metadata.bind.dialect.implicit_returning

    @classmethod
    def create_tables(cls, recreate=False):
        if not cls.is_bootstrapped():
            console.error(
                f'{get_class_name(cls)} cannot create '
                f'tables unless bootstrapped'
            )
            return

        meta = cls.get_metadata()
        engine = cls.get_engine()

        if recreate:
            meta.drop_all(engine)

        # create all tables
        meta.create_all(engine)

        # add a trigger to each table to auto-increment
        # the _rev column on update.
        if cls.dialect == Dialect.postgresql:
            engine.execute(f'''
                drop function if exists increment_rev;
                create function increment_rev() returns trigger
                    language plpgsql
                    as $$
                begin
                    new._rev = old._rev + 1;
                    return new;
                end;
                $$;
            ''')
            for table in meta.tables.values():
                id_col_name = cls.id_column_names[table.name]
                engine.execute(f'''
                    drop trigger if exists increment_{table.name}_rev_on_update
                        on {table.name};

                    create trigger increment_{table.name}_rev_on_update
                    after update on {table.name}
                    for each row
                        when (new.{id_col_name} = old.{id_col_name})
                        execute procedure increment_rev();
                ''')
        else:
            for table in meta.tables.values():
                id_col_name = cls.id_column_names[table.name]
                engine.execute(f'''
                    drop trigger if exists increment_{table.name}_rev_on_update
                        on {table.name};

                    create trigger increment_{table.name}_rev_on_update
                    after update on {table.name}
                    for each row
                    begin
                        update {table.name} set _rev = old._rev + 1
                        where {id_col_name} = old.{id_col_name};
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
