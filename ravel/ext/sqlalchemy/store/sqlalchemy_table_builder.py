from copy import deepcopy

import sqlalchemy as sa

from appyratus.utils import StringUtils

from ravel.constants import REV, ID
from ravel.util.loggers import console


class SqlalchemyTableBuilder(object):
    """
    The role of SqlalchemyTableBuilder is to derive and instantiate a
    Sqlalchemy Table object from a Resource object's schema. This process
    occurs in the Sqlalchemy's on_bootstrap method and assumes that the
    column names in the table map onto each schema's field.source attribute.
    """

    def __init__(self, store: 'SqlalchemyStore'):
        self._dialect = store.dialect
        self._resource_type = store.resource_type
        self._adapters = store.adapters
        self._metadata = store.get_metadata()

    @property
    def adapters(self):
        return self._adapters

    @property
    def dialect(self):
        return self._dialect

    def build_table(self, name=None, schema=None) -> sa.Table:
        """
        Derive a Sqlalchemy Table from a Resource Schema.
        """
        # build the column objects
        is_primary_key_set = False
        id_col = None
        columns = []
        for field in self._resource_type.Schema.fields.values():
            if field.meta.get('ravel_on_resolve'):
                # if ravel_on_resolve is defined, it means that a custom
                # resolver is used to resolve this field's data, so don't
                # create a columns for it
                continue

            col = self.build_column(field)
            columns.append(col)

            if field.name == ID:
                id_col = col
            if col.primary_key:
                is_primary_key_set = True

        # default _id to primary key if not explicitly set
        if not is_primary_key_set:
            id_col.primary_key = True

        # set the table name
        if name is not None:
            table_name = name
        else:
            table_name = StringUtils.snake(self._resource_type.__name__)

        # set database schema, like schema in postgres
        if schema is not None:
            self._metadata.schema = schema

        # finally build and return the SQLAlchemy table object
        console.debug(f'building Sqlalchemy Table: {table_name}')
        table = sa.Table(table_name, self._metadata, *columns)
        return table

    def build_column(self, field: 'Field') -> sa.Column:
        """
        Derive a Sqlalchemy column from a Field object. It looks for
        Sqlalchemy-related column kwargs in the field's meta dict.
        """
        name = field.source

        try:
            dtype = self.adapters.get(field.name).on_adapt(field)
        except:
            console.error(f'could not adapt field {field}')
            raise

        primary_key = field.meta.get('primary_key', False)
        unique = field.meta.get('unique', False)

        if field.source == REV:
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
