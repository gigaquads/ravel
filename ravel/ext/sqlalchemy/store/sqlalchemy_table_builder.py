from typing import Text, Type

import sqlalchemy as sa

from sqlalchemy import ForeignKey
from appyratus.utils.string_utils import StringUtils

from ravel.constants import REV, ID
from ravel.util.loggers import console
from ravel.util.json_encoder import JsonEncoder
from ravel.util.misc_functions import get_class_name
from ravel.schema import fields, Id

json_encoder = JsonEncoder()


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
            table_name = self.derive_table_name(self._resource_type)

        # set database schema, like schema in postgres
        if schema is not None:
            self._metadata.schema = schema

        # finally build and return the SQLAlchemy table object
        table = sa.Table(table_name, self._metadata, *columns)
        return table


    @staticmethod
    def derive_table_name(resource_type: Type['Resource']) -> Text:
        return StringUtils.snake(get_class_name(resource_type))

    def build_column(self, field: 'Field') -> sa.Column:
        """
        Derive a Sqlalchemy column from a Field object. It looks for
        Sqlalchemy-related column kwargs in the field's meta dict.
        """
        name = field.source
        adapter = self.adapters.get(field.name)

        if adapter is None:
            console.warning(
                'no sqlalchemy field adapter registered '
                f'for {field}. using default adapter.'
            )

        try:
            dtype = adapter.on_adapt(field)
        except Exception:
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
            if 'server_default' in field.meta:
                server_default = field.meta['server_default']
            elif field.has_constant_default:
                defaults = self._resource_type.ravel.defaults
                server_default = defaults[field.name]()
            if server_default is not None:
                if not isinstance(server_default, str):
                    if adapter is not None:
                        server_default = adapter.encode(server_default)
                    if isinstance(field, fields.Bool):
                        server_default = 'true' if server_default else 'false'
                    elif isinstance(field, (fields.Int, fields.Float)):
                        server_default = str(server_default)
                    else:
                        try:
                            server_default = json_encoder.encode(
                                server_default
                            )
                        except:
                            server_default = str(server_default)

        # prepare positional arguments for Column ctor
        args = [
            name,
            dtype() if isinstance(dtype, type) else dtype,
        ]
        # foreign key string path, like 'user._id'
        foreign_key_dotted_path = field.meta.get('foreign_key')
        if foreign_key_dotted_path:
            args.append(ForeignKey(foreign_key_dotted_path))

        # prepare keyword arguments for Column ctor
        kwargs = dict(
            index=indexed,
            primary_key=primary_key,
            unique=unique,
            server_default=server_default
        )
        try:
            column = sa.Column(*args, **kwargs)
        except Exception:
            console.error(
                message=f'failed to build sa.Column: {name}',
                data={
                    'args': args,
                    'kwargs': kwargs
                }
            )
            raise

        if field.nullable is not None:
            column.nullable = field.nullable

        return column
