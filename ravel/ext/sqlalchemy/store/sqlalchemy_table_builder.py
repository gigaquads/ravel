from copy import deepcopy

import sqlalchemy as sa

from appyratus.utils import StringUtils

from ravel.constants import REV_FIELD_NAME
from ravel.util.loggers import console


class SqlalchemyTableBuilder(object):
    def __init__(self, store: 'SqlalchemyStore'):
        self._dialect = store.dialect
        self._biz_class = store.biz_class
        self._adapters = store.adapters
        self._metadata = store.get_metadata()

    @property
    def adapters(self):
        return self._adapters

    @property
    def dialect(self):
        return self._dialect

    def build_table(self, name=None, schema=None) -> sa.Table:
        columns = [
            self.build_column(field)
            for field in self._biz_class.Schema.fields.values()
        ]
        if name is not None:
            table_name = name
        else:
            table_name = StringUtils.snake(self._biz_class.__name__)

        console.debug(
            message=(
                f'building Sqlalchemy Table "{table_name}" '
                f'from "{self._biz_class.Schema.__name__}" schema'
            )
        )
        if schema is not None:
            self._metadata.schema = schema
        table = sa.Table(table_name, self._metadata, *columns)
        return table

    def build_column(self, field: 'Field') -> sa.Column:
        name = field.source

        try:
            dtype = self.adapters.get(field.name).on_adapt(field)
        except:
            console.error(f'could not adapt field {field}')
            raise

        primary_key = field.meta.get('primary_key', False)
        meta = field.meta.get('sa', {})
        unique = meta.get('unique', False)

        if field.source == REV_FIELD_NAME:
            indexed = True
            server_default = '0'
        else:
            indexed = field.meta.get('index', False)
            server_default = None

        return sa.Column(
            name,
            dtype() if isinstance(dtype, type) else dtype,
            index=indexed,
            primary_key=primary_key,
            nullable=field.nullable,
            unique=unique,
            server_default=server_default
        )
