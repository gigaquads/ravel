import traceback

import sqlalchemy as sa

from typing import List, Dict, Text, Type, Set, Tuple
from threading import RLock

from geoalchemy2 import Geometry as GeoalchemyGeometry
from sqlalchemy.sql import bindparam
from appyratus.env import Environment

from ravel.query.predicate import (
    Predicate, ConditionalPredicate, BooleanPredicate,
    OP_CODE,
)
from ravel.schema import fields, Field
from ravel.util.loggers import console
from ravel.util.json_encoder import JsonEncoder
from ravel.util import get_class_name
from ravel.store.base import Store
from ravel.constants import REV, ID

from .dialect import Dialect
from .sqlalchemy_table_builder import SqlalchemyTableBuilder
from ..types import ArrayOfEnum, UtcDateTime
from ..postgis import (
    POSTGIS_OP_CODE,
    GeometryObject, PointGeometry, PolygonGeometry,
    Point, Polygon,
)

json_encoder = JsonEncoder()


class SqlalchemyStore(Store):
    """
    A SQLAlchemy-based store, which keeps a single connection pool (AKA
    Engine) shared by all threads; however, each thread keeps singleton
    thread-local database connection and transaction objects, managed through
    connect()/close() and begin()/end().
    """

    env = Environment(
        SQLALCHEMY_STORE_ECHO=fields.Bool(default=False),
        SQLALCHEMY_STORE_SHOW_QUERIES=fields.Bool(default=False),
        SQLALCHEMY_STORE_DIALECT=fields.Enum(
            fields.String(), Dialect.values(), default=Dialect.sqlite
        ),
        SQLALCHEMY_STORE_PROTOCOL=fields.String(default='sqlite'),
        SQLALCHEMY_STORE_USER=fields.String(),
        SQLALCHEMY_STORE_HOST=fields.String(),
        SQLALCHEMY_STORE_PORT=fields.String(),
        SQLALCHEMY_STORE_NAME=fields.String(),
    )

    # id_column_names is a mapping from table name to its _id column name
    _id_column_names = {}

    # only one thread needs to bootstrap the SqlalchemyStore. This lock is
    # used to ensure that this is what happens when the host app bootstraps.
    _bootstrap_lock = RLock()

    @classmethod
    def get_default_adapters(cls, dialect: Dialect, table_name) -> List[Field.Adapter]:
        # TODO: Move this into the adapters file

        adapters = [
            fields.Field.adapt(
                on_adapt=lambda field: sa.Text,
                on_encode=lambda x: cls.ravel.app.json.encode(x),
                on_decode=lambda x: cls.ravel.app.json.decode(x),
            ),
            fields.Email.adapt(on_adapt=lambda field: sa.Text),
            fields.Bytes.adapt(on_adapt=lambda field: sa.LargeBinary),
            fields.BcryptString.adapt(on_adapt=lambda field: sa.Text),
            fields.Float.adapt(on_adapt=lambda field: sa.Float),
            fields.DateTime.adapt(on_adapt=lambda field: UtcDateTime),
            fields.Timestamp.adapt(on_adapt=lambda field: UtcDateTime),
            fields.Bool.adapt(on_adapt=lambda field: sa.Boolean),
            fields.TimeDelta.adapt(on_adapt=lambda field: sa.Interval),
            fields.Enum.adapt(
                on_adapt=lambda field: {
                    fields.String: sa.Text,
                    fields.Int: sa.Integer,
                    fields.Float: sa.Float,
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
                fields.Int, fields.Uint32, fields.Uint64, fields.Uint,
                fields.Int32,
            }
        )
        if dialect == Dialect.postgresql:
            adapters.extend(cls.get_postgresql_default_adapters(table_name))
        elif dialect == Dialect.mysql:
            adapters.extend(cls.get_mysql_default_adapters(table_name))
        elif dialect == Dialect.sqlite:
            adapters.extend(cls.get_sqlite_default_adapters(table_name))

        return adapters

    @classmethod
    def get_postgresql_default_adapters(cls, table_name) -> List[Field.Adapter]:
        pg_types = sa.dialects.postgresql

        def on_adapt_list(field):
            if isinstance(field.nested, fields.Enum):
                name = f'{table_name}__{field.name}'
                return ArrayOfEnum(
                    pg_types.ENUM(*field.nested.values, name=name)
                )
            return pg_types.ARRAY({
                fields.String: sa.Text,
                fields.Email: sa.Text,
                fields.Uuid: pg_types.UUID,
                fields.Int: sa.Integer,
                fields.Bool: sa.Boolean,
                fields.Float: sa.Float,
                fields.DateTime: UtcDateTime,
                fields.Timestamp: UtcDateTime,
                fields.Dict: pg_types.JSONB,
                fields.Field: pg_types.JSONB,
                fields.Nested: pg_types.JSONB,
            }.get(type(field.nested), sa.Text))

        return [
            Point.adapt(
                on_adapt=lambda field: GeoalchemyGeometry(field.geo_type),
                on_encode=lambda x: x.to_EWKT_string(),
                on_decode=lambda x: (
                    PointGeometry(x['geometry']['coordinates']) if x
                    else None
                )
            ),
            Polygon.adapt(
                on_adapt=lambda field: GeoalchemyGeometry(field.geo_type),
                on_encode=lambda x: x.to_EWKT_string(),
                on_decode=lambda x: PolygonGeometry(
                    x['geometry']['coordinates'] if x
                    else None
                )
            ),
            fields.Field.adapt(on_adapt=lambda field: pg_types.JSONB),
            fields.Uuid.adapt(on_adapt=lambda field: pg_types.UUID),
            fields.Dict.adapt(on_adapt=lambda field: pg_types.JSONB),
            fields.Nested.adapt(
                on_adapt=lambda field: pg_types.JSONB,
            ),
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
    def get_mysql_default_adapters(cls, table_name) -> List[Field.Adapter]:
        return [
            fields.Dict.adapt(on_adapt=lambda field: sa.JSON),
            fields.Nested.adapt(on_adapt=lambda field: sa.JSON),
            fields.List.adapt(on_adapt=lambda field: sa.JSON),
            fields.Set.adapt(
                on_adapt=lambda field: sa.JSON,
                on_encode=lambda x: cls.ravel.app.json.encode(x),
                on_decode=lambda x: set(cls.ravel.app.json.decode(x))
            ),
        ]

    @classmethod
    def get_sqlite_default_adapters(cls, table_name) -> List[Field.Adapter]:
        adapters = [
            field_class.adapt(
                on_adapt=lambda field: sa.Text,
                on_encode=lambda x: cls.ravel.app.json.encode(x),
                on_decode=lambda x: cls.ravel.app.json.decode(x),
            )
            for field_class in {
                fields.Dict, fields.List, fields.Nested
            }
        ]
        adapters.append(
            fields.Set.adapt(
                on_adapt=lambda field: sa.Text,
                on_encode=lambda x: cls.ravel.app.json.encode(x),
                on_decode=lambda x: set(cls.ravel.app.json.decode(x))
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
        self._options = {}

    @property
    def adapters(self):
        return self._adapters

    @property
    def id_column_name(self):
        return self.resource_type.Schema.fields[ID].source

    def prepare(self, record: Dict, serialize=True) -> Dict:
        """
        When inserting or updating data, the some raw values in the record
        dict must be transformed before their corresponding sqlalchemy column
        type will accept the data.
        """
        cb_name = 'on_encode' if serialize else 'on_decode'
        prepared_record = {}
        for k, v in record.items():
            if k in (REV):
                prepared_record[k] = v
            adapter = self._adapters.get(k)
            if adapter:
                callback = getattr(adapter, cb_name, None)
                if callback:
                    try:
                        prepared_record[k] = callback(v)
                        continue
                    except Exception:
                        console.error(
                            message=f'failed to adapt column value: {k}',
                            data={
                                'value': v,
                                'field': adapter.field_class
                            }
                        )
                        raise
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
    def on_bootstrap(cls, url=None, dialect=None, echo=False, db=None, **kwargs):
        """
        Initialize the SQLAlchemy connection pool (AKA Engine).
        """
        with cls._bootstrap_lock:
            cls.ravel.kwargs = kwargs

            # construct the URL to the DB server
            # url can be a string or a dict
            if isinstance(url, dict):
                url_parts = url
                cls.ravel.app.shared.sqla_url = (
                    '{protocol}://{user}@{host}:{port}/{db}'.format(
                        **url_parts
                    )
                )
            elif isinstance(url, str):
                cls.ravel.app.shared.sqla_url = url
            else:
                url_parts = dict(
                    protocol=cls.env.SQLALCHEMY_STORE_PROTOCOL,
                    user=cls.env.SQLALCHEMY_STORE_USER or '',
                    host=(
                        '@' + cls.env.SQLALCHEMY_STORE_HOST
                        if cls.env.SQLALCHEMY_STORE_HOST else ''
                    ),
                    port=(
                        ':' + cls.env.SQLALCHEMY_STORE_PORT
                        if cls.env.SQLALCHEMY_STORE_PORT else ''
                    ),
                    db=(
                        '/' + (db or cls.env.SQLALCHEMY_STORE_NAME or '')
                    )
                )
                cls.ravel.app.shared.sqla_url = url or (
                    '{protocol}://{user}{host}{port}{db}'.format(
                        **url_parts
                    )
                )

            cls.dialect = dialect or cls.env.SQLALCHEMY_STORE_DIALECT

            from sqlalchemy.dialects import postgresql, sqlite, mysql

            cls.sa_dialect = None
            if cls.dialect == Dialect.postgresql:
                cls.sa_dialect = postgresql
            elif cls.dialect == Dialect.sqlite:
                cls.sa_dialect = sqlite
            elif cls.dialect == Dialect.mysql:
                cls.sa_dialect = mysql

            console.debug(
                message='creating sqlalchemy engine',
                data={
                    'echo': cls.env.SQLALCHEMY_STORE_ECHO,
                    'dialect': cls.dialect,
                    'url': cls.ravel.app.shared.sqla_url,
                }
            )

            cls.ravel.local.sqla_tx = None
            cls.ravel.local.sqla_conn = None
            cls.ravel.local.sqla_metadata = sa.MetaData()
            cls.ravel.local.sqla_metadata.bind = sa.create_engine(
                name_or_url=cls.ravel.app.shared.sqla_url,
                echo=bool(echo or cls.env.SQLALCHEMY_STORE_ECHO),
                strategy='threadlocal'
            )

            # set global thread-local sqlalchemy store method aliases
            cls.ravel.app.local.create_tables = cls.create_tables

    def on_bind(
        self,
        resource_type: Type['Resource'],
        table: Text = None,
        schema: 'Schema' = None,
        **kwargs
    ):
        """
        Initialize SQLAlchemy data strutures used for constructing SQL
        expressions used to manage the bound resource type.
        """
        # map each of the resource's schema fields to a corresponding adapter,
        # which is used to prepare values upon insert and update.
        table = (
            table or SqlalchemyTableBuilder.derive_table_name(resource_type)
        )
        field_class_2_adapter = {
            adapter.field_class: adapter for adapter in
            self.get_default_adapters(self.dialect, table) + self._custom_adapters
        }
        self._adapters = {
            field_name: field_class_2_adapter[type(field)]
            for field_name, field in self.resource_type.Schema.fields.items()
            if (
                type(field) in field_class_2_adapter and
                field.meta.get('ravel_on_resolve') is None
            )
        }

        # build the Sqlalchemy Table object for the bound resource type.
        self._builder = SqlalchemyTableBuilder(self)

        try:
            self._table = self._builder.build_table(name=table, schema=schema)
        except Exception:
            console.error(f'failed to build sa.Table: {table}')
            raise

        self._id_column = getattr(self._table.c, self.id_column_name)

        # remember which column is the _id column
        self._id_column_names[self._table.name] = self.id_column_name

        # set SqlalchemyStore options here, using bootstrap-level
        # options as base/default options.
        self._options = dict(self.ravel.kwargs, **kwargs)

    def query(
        self,
        predicate: 'Predicate',
        fields: Set[Text] = None,
        limit: int = None,
        offset: int = None,
        order_by: Tuple = None,
        **kwargs,
    ):
        fields = fields or {
            k: None for k in self._adapters
        }
        fields.update({
            self.id_column_name: None,
            self.resource_type.Schema.fields[REV].source: None,
        })

        columns = []
        table_alias = self.table.alias(
            ''.join(s.strip('_')[0] for s in self.table.name.split('_'))
        )
        for k in fields:
            col = getattr(table_alias.c, k)
            if isinstance(col.type, GeoalchemyGeometry):
                columns.append(sa.func.ST_AsGeoJSON(col).label(k))
            else:
                columns.append(col)

        predicate = Predicate.deserialize(predicate)
        filters = self._prepare_predicate(table_alias, predicate)

        # build the query object
        query = sa.select(columns).where(filters)

        if order_by:
            sa_order_by = [
                sa.desc(getattr(table_alias.c, x.key)) if x.desc else
                sa.asc(getattr(table_alias.c, x.key))
                for x in order_by
            ]
            query = query.order_by(*sa_order_by)

        if limit is not None:
            query = query.limit(max(0, limit))
        if offset is not None:
            query = query.offset(max(0, limit))

        console.debug(
            message=(
                f'SQL: SELECT FROM {self.table}'
                + (f' OFFSET {offset}' if offset is not None else '')
                + (f' LIMIT {limit}' if limit else '')
                + (f' ORDER BY {", ".join(x.to_sql() for x in order_by)}'
                    if order_by else '')
            ),
            data={
                'stack': traceback.format_stack(),
                'statement': str(query.compile(
                    compile_kwargs={'literal_binds': True}
                )).split('\n')
            }
            if self.env.SQLALCHEMY_STORE_SHOW_QUERIES
            else None
        )
        # execute query, aggregating resulting records
        cursor = self.conn.execute(query)
        records = []

        while True:
            page = [
                self.prepare(dict(row.items()), serialize=False)
                for row in cursor.fetchmany(512)
            ]
            if page:
                records.extend(page)
            else:
                break

        return records

    def _prepare_predicate(self, table, pred, empty=set()):
        if isinstance(pred, ConditionalPredicate):
            if not pred.ignore_field_adapter:
                adapter = self._adapters.get(pred.field.source)
                if adapter and adapter.on_encode:
                    pred.value = adapter.on_encode(pred.value)
            col = getattr(table.c, pred.field.source)
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
            elif pred.op == POSTGIS_OP_CODE.CONTAINS:
                if isinstance(pred.value, GeometryObject):
                    EWKT_str = pred.value.to_EWKT_string()
                else:
                    EWKT_str = pred.value
                return sa.func.ST_Contains(
                    col, sa.func.ST_GeomFromEWKT(EWKT_str),
                )
            elif pred.op == POSTGIS_OP_CODE.CONTAINED_BY:
                if isinstance(pred.value, GeometryObject):
                    EWKT_str = pred.value.to_EWKT_string()
                else:
                    EWKT_str = pred.value
                return sa.func.ST_Contains(
                    sa.func.ST_GeomFromEWKT(EWKT_str), col
                )
            elif pred.op == POSTGIS_OP_CODE.WITHIN_RADIUS:
                center = pred.value['center']
                radius = pred.value['radius']
                return sa.func.ST_PointInsideCircle(
                    col, center[0], center[1], radius
                )
            else:
                raise Exception('unrecognized conditional predicate')
        elif isinstance(pred, BooleanPredicate):
            if pred.op == OP_CODE.AND:
                lhs_result = self._prepare_predicate(table, pred.lhs)
                rhs_result = self._prepare_predicate(table, pred.rhs)
                return sa.and_(lhs_result, rhs_result)
            elif pred.op == OP_CODE.OR:
                lhs_result = self._prepare_predicate(table, pred.lhs)
                rhs_result = self._prepare_predicate(table, pred.rhs)
                return sa.or_(lhs_result, rhs_result)
            else:
                raise Exception('unrecognized boolean predicate')
        else:
            raise Exception('unrecognized predicate type')

    def exists(self, _id) -> bool:
        columns = [sa.func.count(self._id_column)]
        query = (
            sa.select(columns).where(
                self._id_column == self.adapt_id(_id)
            )
        )
        result = self.conn.execute(query)
        return bool(result.scalar())

    def exists_many(self, _ids: Set) -> Dict[object, bool]:
        columns = [self._id_column, sa.func.count(self._id_column)]
        query = (
            sa.select(columns).where(
                self._id_column.in_(
                    [self.adapt_id(_id) for _id in _ids]
                )
            )
        )
        return {
            row[0]: row[1] for row in self.conn.execute(query)
        }

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
                f.source for f in self.resource_type.Schema.fields.values()
                if f.name in self._adapters
            }
        fields.update({
            self.id_column_name,
            self.resource_type.Schema.fields[REV].source,
        })

        columns = []
        for k in fields:
            col = getattr(self.table.c, k)
            if isinstance(col.type, GeoalchemyGeometry):
                columns.append(sa.func.ST_AsGeoJSON(col).label(k))
            else:
                columns.append(col)

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
                    record = self.prepare(raw_record, serialize=False)
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
        prepared_record = self.prepare(record, serialize=True)
        insert_stmt = self.table.insert().values(**prepared_record)
        _id = prepared_record.get('_id', '')
        console.debug(
            f'SQL: INSERT {str(_id)[:7] + " " if _id else ""}'
            f'INTO {self.table}'
        )
        try:
            if self.supports_returning:
                insert_stmt = insert_stmt.return_defaults()
                result = self.conn.execute(insert_stmt)
                return dict(record, **(result.returned_defaults or {}))
            else:
                result = self.conn.execute(insert_stmt)
                return self.fetch(_id=record[self.id_column_name])
        except Exception:
            console.error(
                message=f'failed to insert record',
                data={
                    'record': record,
                    'resource': get_class_name(self.resource_type),
                }
            )
            raise

    def create_many(self, records: List[Dict]) -> Dict:
        prepared_records = []
        nullable_fields = self.resource_type.Schema.nullable_fields
        for record in records:
            record[self.id_column_name] = self.create_id(record)
            prepared_record = self.prepare(record, serialize=True)
            prepared_records.append(prepared_record)
            for nullable_field in nullable_fields.values():
                if nullable_field.name not in prepared_record:
                    prepared_record[nullable_field.name] = None


        try:
            self.conn.execute(self.table.insert(), prepared_records)
        except Exception:
            console.error(f'failed to insert records')
            raise

        n = len(prepared_records)
        id_list_str = (
            ', '.join(str(x['_id'])[:7]
            for x in prepared_records if x.get('_id'))
        )
        console.debug(
            f'SQL: INSERT {id_list_str} INTO {self.table} '
            + (f'(count: {n})' if n > 1 else '')
        )

        if self.supports_returning:
            # TODO: use implicit returning if possible
            pass

        return self.fetch_many(
            (rec[self.id_column_name] for rec in records), as_list=True)

    def update(self, _id, data: Dict) -> Dict:
        prepared_id = self.adapt_id(_id)
        prepared_data = self.prepare(data, serialize=True)
        if prepared_data:
            update_stmt = (
                self.table
                    .update()
                    .values(**prepared_data)
                    .where(self._id_column == prepared_id)
                )
        else:
            return prepared_data
        if self.supports_returning:
            update_stmt = update_stmt.return_defaults()
            console.debug(f'SQL: UPDATE {self.table}')
            result = self.conn.execute(update_stmt)
            return dict(data, **(result.returned_defaults or {}))
        else:
            self.conn.execute(update_stmt)
            if self._options.get('fetch_on_update', True):
                return self.fetch(_id)
            return data

    def update_many(self, _ids: List, data: List[Dict] = None) -> None:
        assert data

        prepared_ids = []
        prepared_records = []

        for _id, record in zip(_ids, data):
            prepared_id = self.adapt_id(_id)
            prepared_record = self.prepare(record, serialize=True)
            if prepared_record:
                prepared_ids.append(prepared_id)
                prepared_records.append(prepared_record)
                prepared_record[ID] = prepared_id

        if prepared_records:
            n = len(prepared_records)
            console.debug(
                f'SQL: UPDATE {self.table} '
                + (f'({n}x)' if n > 1else '')
            )
            values = {
                k: bindparam(k) for k in prepared_records[0].keys()
            }
            update_stmt = (
                self.table
                    .update()
                    .where(self._id_column == bindparam(self.id_column_name))
                    .values(**values)
            )
            self.conn.execute(update_stmt, prepared_records)

        if self._options.get('fetch_on_update', True):
            if self.supports_returning:
                # TODO: use implicit returning if possible
                return self.fetch_many(_ids)
            else:
                return self.fetch_many(_ids)
        return

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
        sqla_conn = getattr(self.ravel.local, 'sqla_conn', None)
        if sqla_conn is None:
            # lazily initialize a connection for this thread
            self.connect()
        return self.ravel.local.sqla_conn

    @property
    def supports_returning(self):
        if not self.is_bootstrapped():
            return False
        metadata = self.get_metadata()
        return metadata.bind.dialect.implicit_returning

    @classmethod
    def create_tables(cls, overwrite=False):
        """
        Create all tables for all SqlalchemyStores used in the host app.
        """
        if not cls.is_bootstrapped():
            console.error(
                f'{get_class_name(cls)} cannot create '
                f'tables unless bootstrapped'
            )
            return

        meta = cls.get_metadata()
        engine = cls.get_engine()

        if overwrite:
            console.info('dropping Resource SQL tables...')
            meta.drop_all(engine)

        # create all tables
        console.info('creating Resource SQL tables...')
        meta.create_all(engine)

    @classmethod
    def get_active_connection(cls):
        return getattr(cls.ravel.local, 'sqla_conn', None)

    @classmethod
    def connect(cls, refresh=True):
        """
        Create a singleton thread-local SQLAlchemy connection, shared across
        all Resources backed by a SQLAlchemy store. When working with multiple
        threads or processes, make sure to 
        """
        sqla_conn = getattr(cls.ravel.local, 'sqla_conn', None)
        metadata = cls.ravel.local.sqla_metadata
        if sqla_conn is not None:
            console.warning(
                message='sqlalchemy store already has connection',
            )
            if refresh:
                cls.close()
                cls.ravel.local.sqla_conn = metadata.bind.connect()
        else:
            cls.ravel.local.sqla_conn = metadata.bind.connect()

        return cls.ravel.local.sqla_conn

    @classmethod
    def close(cls):
        """
        Return the thread-local database connection to the sqlalchemy
        connection pool (AKA the "engine").
        """
        sqla_conn = getattr(cls.ravel.local, 'sqla_conn', None)
        if sqla_conn is not None:
            console.debug('closing sqlalchemy connection')
            sqla_conn.close()
            cls.ravel.local.sqla_conn = None
        else:
            console.warning('sqlalchemy has no connection to close')

    @classmethod
    def begin(cls, auto_connect=True, **kwargs):
        """
        Initialize a thread-local transaction. An exception is raised if
        there's already a pending transaction.
        """
        conn = cls.get_active_connection()
        if conn is None:
            if auto_connect:
                conn = cls.connect()
            else:
                raise Exception('no active sqlalchemy connection')

        existing_tx = getattr(cls.ravel.local, 'sqla_tx', None)
        if existing_tx is not None:
            console.debug('there is already an open transaction')
        else:
            new_tx = cls.ravel.local.sqla_conn.begin()
            cls.ravel.local.sqla_tx = new_tx

    @classmethod
    def commit(cls, rollback=True, **kwargs):
        """
        Call commit on the thread-local database transaction. "Begin" must be
        called to start a new transaction at this point, if a new transaction
        is desired.
        """
        def perform_sqlalchemy_commit():
            tx = getattr(cls.ravel.local, 'sqla_tx', None)
            if tx is not None:
                cls.ravel.local.sqla_tx.commit()
                cls.ravel.local.sqla_tx = None

        # try to commit the transaction.
        console.debug(f'committing sqlalchemy transaction')
        try:
            perform_sqlalchemy_commit()
        except Exception:
            if rollback:
                # if the commit fails, rollback the transaction
                console.critical(
                    f'rolling back sqlalchemy transaction'
                )
                cls.rollback()
            else:
                console.exception(
                    f'sqlalchemy transaction failed commit'
                )
        finally:
            # ensure we close the connection either way
            cls.close()

    @classmethod
    def rollback(cls, **kwargs):
        tx = getattr(cls.ravel.local, 'sqla_tx', None)
        if tx is not None:
            cls.ravel.local.sqla_tx = None
            try:
                tx.rollback()
            except:
                console.exception(
                    f'sqlalchemy transaction failed to rollback'
                )

    @classmethod
    def has_transaction(cls) -> bool:
        return cls.ravel.local.sqla_tx is not None

    @classmethod
    def get_metadata(cls):
        return cls.ravel.local.sqla_metadata

    @classmethod
    def get_engine(cls):
        return cls.get_metadata().bind

    @classmethod
    def dispose(cls):
        meta = cls.get_metadata()
        if not meta:
            cls.ravel.local.sqla_metadata = None
            return

        engine = meta.bind
        engine.dispose()
