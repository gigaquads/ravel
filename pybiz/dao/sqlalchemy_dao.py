import re
import threading

import sqlalchemy as sa

from copy import deepcopy
from typing import List, Dict, Text

from appyratus.memoize import memoized_property

from pybiz.predicate import Predicate, ConditionalPredicate, BooleanPredicate

from .base import Dao

# TODO: Move thread-local SQLAlchemy data management into a Profile class.

class SqlalchemyDao(Dao):

    @classmethod
    def __table__(cls):
        raise NotImplementedError()

    @classmethod
    def create_engine(cls):
        cls.local.engine = sa.create_engine(cls.url, echo=cls.echo)

    @classmethod
    def create_tables(cls):
        cls.get_metadata().create_all(cls.local.engine)

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

    @classmethod
    def get_metadata(cls):
        return cls.metadata

    @classmethod
    def get_table(cls):
        if cls._table is None:
            cls._table = cls.__table__()
        return cls._table

    @classmethod
    def factory(cls, name, url: Text, meta: sa.MetaData = None, echo=False):
        derived_type = type(name, (SqlalchemyDao, ), {})
        derived_type.url = url
        derived_type.echo = echo
        derived_type.metadata = meta or sa.MetaData()
        derived_type.local = threading.local()
        derived_type.local.engine = None
        derived_type.local.connection = None

        # _table class attr is set by subclasses and accessed
        # via calls to cls.get_table() or the self.table property
        derived_type._table = None

        return derived_type

    @property
    def table(self):
        return self.get_table()

    @property
    def conn(self):
        return self.local.connection

    @property
    def supports_returning(self):
        return self.local.engine.dialect.implicit_returning

    def __init__(self):
        super().__init__()
        if self.table.metadata.bind is None:
            self.table.metadata.bind = self.local.engine

    def next_id(self, record: Dict) -> object:
        # id generation should occurs in the database, not here.
        raise NotImplementedError()

    def query(
        self,
        predicate,
        fields=None,
        limit=None,
        offset=None,
        order_by=None,  # TODO: implement order_by
        **kwargs,
    ):
        fields = fields or self.bizobj_type.schema.fields.keys()
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
        fields = set(fields or self.bizobj_type.schema.keys())
        fields.update(['_id', '_rev'])
        columns = [getattr(self.table.c, k) for k in fields]
        select_stmt = sa.select(columns).where(self.table.c._id.in_(_ids))
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
        raise NotImplementedError()

    def create(self, record: dict) -> dict:
        insert_stmt = self.table.insert().values(**record)
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
        else:
            self.conn.execute(update_stmt)
            return None

    def update_many(self, _ids: List, data: Dict = None) -> None:
        assert data
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
