import re

import sqlalchemy as sa
import pytz

from sqlalchemy import TypeDecorator
from sqlalchemy.dialects.postgresql import ARRAY


class ArrayOfEnum(TypeDecorator):
    """
    See: https://docs.sqlalchemy.org/en/13/dialects/postgresql.html
    """

    impl = ARRAY
    re_pg_array = re.compile(r'^{(.*)}$')

    def bind_expression(self, bindvalue):
        return sa.cast(bindvalue, self)

    def result_processor(self, dialect, coltype):
        super_rp = super().result_processor(dialect, coltype)

        def processor(value):
            result = []
            if value is not None:
                result = super_rp(self._handle_raw_string(value))
            return result

        return processor

    def _handle_raw_string(self, value):
        match = self.re_pg_array.match(value)
        processed_values = []
        if match:
            inner = match.group(1)
            if inner:
                processed_values = inner.split(',')
        return processed_values


class UtcDateTime(sa.TypeDecorator):

    impl = sa.DateTime

    def process_result_value(self, value, dialect):
        if value:
            return value.replace(tzinfo=pytz.utc)
        return value

