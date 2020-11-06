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
from ravel.util import get_class_name
from ravel.store.base import Store
from ravel.constants import REV, ID

from .dialect import Dialect
from .sqlalchemy_table_builder import SqlalchemyTableBuilder
from ..types import ArrayOfEnum
from ..postgis import (
    POSTGIS_OP_CODE,
    GeometryObject, PointGeometry, PolygonGeometry,
    Geometry, Point, Polygon,
)


class SqlalchemyStore(Store):
    """
    A SQLAlchemy-based store, which keeps a single connection pool (AKA
    Engine) shared by all threads; however, each thread keeps singleton
    thread-local database connection and transaction objects, managed through
    connect()/close() and begin()/end().
    """

    env = Environment(
        SQLALCHEMY_STORE_ECHO=fields.Bool(default=False),
        SQLALCHEMY_STORE_DIALECT=fields.Enum(
            fields.String(), Dialect.values(), default=Dialect.sqlite
        ),
        SQLALCHEMY_STORE_PROTOCOL=fields.String(default='sqlite'),
        SQLALCHEMY_STORE_USER=fields.String(default='admin'),
        SQLALCHEMY_STORE_HOST=fields.String(default='0.0.0.0'),
        SQLALCHEMY_STORE_PORT=fields.Int(default=5432),
        SQLALCHEMY_STORE_NAME=fields.String(),
    )

    # id_column_names is a mapping from table name to its _id column name
    _id_column_names = {}

    # only one thread needs to bootstrap the SqlalchemyStore. This lock is
    # used to ensure that this is what happens when the host app bootstraps.
    _bootstrap_lock = RLock()

    @classmethod
    def get_default_adapters(cls, dialect: Dialect) -> List[Field.Adapter]:
        # TODO: Move this into the adapters file
        adapters = [
            fields.Field.adapt(
                on_adapt=lambda field: sa.Text,
                on_encode=lambda x: cls.ravel.app.json.encode(x),
                on_decode=lambda x: cls.ravel.app.json.decode(x),
            ),
            fields.Email.adapt(on_adapt=lambda field: sa.Text),
            fields.Float.adapt(on_adapt=lambda field: sa.Float),
            fields.Bool.adapt(on_adapt=lambda field: sa.Boolean),
            fields.DateTime.adapt(on_adapt=lambda field: sa.DateTime),
            fields.Timestamp.adapt(on_adapt=lambda field: sa.DateTime),
            fields.Enum.adapt(on_adapt=lambda field: {
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
                fields.Email: sa.Text,
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
            Point.adapt(
                on_adapt=lambda field: GeoalchemyGeometry(field.geo_type),
                on_encode=lambda x: x.to_EWKT_string(),
                on_decode=lambda x: PointGeometry(x['geometry']['coordinates'])  # TODO: extracvt vertices from GeoJSON
            ),
            Polygon.adapt(
                on_adapt=lambda field: GeoalchemyGeometry(field.geo_type),
                on_encode=lambda x: x.to_EWKT_string(),
                on_decode=lambda x: PolygonGeometry(x['geometry']['coordinates'])  # TODO: extracvt vertices from GeoJSON
            ),
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
                on_encode=lambda x: cls.ravel.app.json.encode(x),
                on_decode=lambda x: set(cls.ravel.app.json.decode(x))
            ),
        ]

    @classmethod
    def get_sqlite_default_adapters(cls) -> List[Field.Adapter]:
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
        """
        Initialize the SQLAlchemy connection pool (AKA Engine).
        """
        with cls._bootstrap_lock:
            if cls.is_bootstrapped():
                return

            # construct the URL to the DB server
            # url can be a string or a dict
            if isinstance(url, dict):
                url_parts = url
                cls.ravel.app.local.sqla_url = (
                    '{protocol}://{user}@{host}:{port}/{db}'.format(
                        **url_parts
                    )
                )
            elif isinstance(url, str):
                cls.ravel.app.local.sqla_url = url
            else:
                url_parts = dict(
                    protocol=cls.env.SQLALCHEMY_STORE_PROTOCOL,
                    user=cls.env.SQLALCHEMY_STORE_USER,
                    host=cls.env.SQLALCHEMY_STORE_HOST,
                    port=cls.env.SQLALCHEMY_STORE_PORT,
                    db=cls.env.SQLALCHEMY_STORE_NAME,
                )
                cls.ravel.app.local.sqla_url = url or (
                    '{protocol}://{user}@{host}:{port}/{db}'.format(
                        **url_parts
                    )
                )

            cls.dialect = dialect or cls.env.SQLALCHEMY_STORE_DIALECT

            console.info(
                message='creating Sqlalchemy engine',
                data={
                    'echo': cls.env.SQLALCHEMY_STORE_ECHO,
                    'dialect': cls.dialect,
                    'url': cls.ravel.app.local.sqla_url,
                }
            )

            cls.ravel.app.local.sqla_metadata = sa.MetaData()
            cls.ravel.app.local.sqla_metadata.bind = sa.create_engine(
                name_or_url=cls.ravel.app.local.sqla_url,
                echo=bool(echo or cls.env.SQLALCHEMY_STORE_ECHO),
            )

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
        field_class_2_adapter = {
            adapter.field_class: adapter for adapter in
            self.get_default_adapters(self.dialect) + self._custom_adapters
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
        self._table = self._builder.build_table(name=table, schema=schema)
        self._id_column = getattr(self._table.c, self.id_column_name)

        # remember which column is the _id column
        self._id_column_names[self._table.name] = self.id_column_name

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
        for k in fields:
            col = getattr(self.table.c, k)
            if isinstance(col.type, GeoalchemyGeometry):
                columns.append(sa.func.ST_AsGeoJSON(col))
            else:
                columns.append(col)

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

        console.debug(
            message='executing SQL query',
            data={
                'query': str(query.compile(
                    compile_kwargs={'literal_binds': True}
                )).split('\n')
            }
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
                columns.append(sa.func.ST_AsGeoJSON(col))
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
            prepared_record = self.prepare(record, serialize=True)
            prepared_records.append(prepared_record)

        self.conn.execute(self.table.insert(), prepared_records)
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
        if self.supports_returning:
            update_stmt = update_stmt.return_defaults()
            result = self.conn.execute(update_stmt)
            return dict(data, **(result.returned_defaults or {}))
        else:
            self.conn.execute(update_stmt)
            return self.fetch(_id)

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

        if prepared_records:
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

        if self.supports_returning:
            # TODO: use implicit returning if possible
            return self.fetch_many(_ids)
        else:
            return self.fetch_many(_ids)

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
        sqla_conn = getattr(self.ravel.app.local, 'sqla_conn', None)
        if sqla_conn is None:
            # lazily initialize a connection for this thread
            self.connect()
        return self.ravel.app.local.sqla_conn

    @property
    def supports_returning(self):
        if not self.is_bootstrapped():
            return False
        metadata = self.get_metadata()
        return metadata.bind.dialect.implicit_returning

    @classmethod
    def create_tables(cls, recreate=False):
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

        if recreate:
            meta.drop_all(engine)

        # create all tables
        meta.create_all(engine)

    @classmethod
    def connect(cls):
        """
        """
        sqla_conn = getattr(cls.ravel.app.local, 'sqla_conn', None)
        if sqla_conn is not None:
            console.warning(
                message='sqlalchemy store already has connection',
            )
        metadata = cls.ravel.app.local.sqla_metadata
        cls.ravel.app.local.sqla_conn = metadata.bind.connect()

    @classmethod
    def close(cls):
        """
        Return the thread-local database connection to the sqlalchemy
        connection pool (AKA the "engine").
        """
        sqla_conn = getattr(cls.ravel.app.local, 'sqla_conn', None)
        if sqla_conn is not None:
            sqla_conn.close()
            cls.ravel.app.local.sqla_conn = None
        else:
            console.warning(
                message='sqlalchemy has no connection to close',
                data={
                    'store': get_class_name(cls)
                }
            )

    @classmethod
    def begin(cls):
        """
        Initialize a thread-local transaction. An exception is raised if
        there's already a pending transaction.
        """
        tx = getattr(cls.ravel.app.local, 'sqla_tx', None)
        if tx is not None:
            raise Exception('there is already an open transaction')
        cls.ravel.app.local.sqla_tx = cls.ravel.app.local.sqla_conn.begin()

    @classmethod
    def commit(cls):
        """
        Call commit on the thread-local database transaction. "Begin" must be
        called to start a new transaction at this point, if a new transaction
        is desired.
        """
        tx = getattr(cls.ravel.app.local, 'sqla_tx', None)
        if tx is not None:
            cls.ravel.app.local.sqla_tx.commit()
            cls.ravel.app.local.sqla_tx = None

    @classmethod
    def rollback(cls):
        tx = getattr(cls.ravel.app.local, 'sqla_tx', None)
        if tx is not None:
            cls.ravel.app.local.sqla_tx = None
            tx.rollback()

    @classmethod
    def get_metadata(cls):
        return cls.ravel.app.local.sqla_metadata

    @classmethod
    def get_engine(cls):
        return cls.get_metadata().bind
