import re
import threading
import uuid
import pickle


import sqlalchemy as sa

from copy import deepcopy
from typing import List, Dict, Text, Type, Set, Tuple

from appyratus.memoize import memoized_property
from appyratus.utils import StringUtils
from appyratus.enum import EnumValueStr
from appyratus.env import Environment

from pybiz.app.middleware import ApplicationMiddleware
from pybiz.util.json_encoder import JsonEncoder
from pybiz.schema import fields, Field

from .dialect import Dialect


class SqlalchemyTableBuilder(object):
    def __init__(self, dao: 'SqlalchemyDao'):
        self._dialect = dao.dialect
        self._biz_class = dao.biz_class
        self._adapters = dao.adapters
        self._metadata = deepcopy(dao.get_metadata())

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
        if schema is not None:
            self._metadata.schema = schema
        table = sa.Table(table_name, self._metadata, *columns)
        return table

    def build_column(self, field: Field) -> sa.Column:
        name = field.source
        dtype = self.adapters.get(field.name).on_adapt(field)
        primary_key = field.meta.get('primary_key', False)
        meta = field.meta.get('sa', {})
        unique = meta.get('unique', False)

        if field.source == '_rev':
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
