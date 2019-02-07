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

from pybiz.api.middleware import RegistryMiddleware
from pybiz.predicate import Predicate, ConditionalPredicate, BooleanPredicate
from pybiz.schema import fields, Field
from pybiz.util import JsonEncoder

from .dialect import Dialect


class SqlalchemyTableBuilder(object):
    def __init__(self, dao: 'SqlalchemyDao'):
        self._dialect = dao.dialect
        self._biz_type = dao.biz_type
        self._metadata = dao.get_metadata()
        self._adapters = dao.adapters

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
        dtype = self.adapters.get(field.source).on_adapt(field)
        primary_key = field.name == '_id'
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
            dtype(),
            index=indexed,
            primary_key=primary_key,
            nullable=field.nullable,
            unique=unique,
            server_default=server_default
        )
