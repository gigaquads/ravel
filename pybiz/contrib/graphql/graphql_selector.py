from typing import Text, Dict, Callable, Tuple
from mock import MagicMock

from appyratus.schema import Schema

import pybiz.biz

from pybiz.util.misc_functions import normalize_to_tuple
from pybiz.biz.biz_attribute import BizAttribute


class GraphQLSelector(BizAttribute):
    def __init__(
        self,
        target: Callable,
        select: Callable = None,
        where: Callable = None,
        order_by: Callable = None,
        offset: int = None,
        limit: int = None,
        first: bool = False,
    ):
        super().__init__()
        self._target_func = target
        self._select_func = select
        self._where_func = where
        self._order_by_func = order_by
        self._offset = offset
        self._limit = limit
        self._first = bool(first)
        self._target_biz_type = None

        if not select:
            self._select_func = lambda caller: (
                self._target_biz_type.schema.fields.keys()
            )

    @property
    def order_key(self):
        return 100

    @property
    def category(self):
        return 'graphql_selector'

    @property
    def target_biz_type(self):
        return self._target_biz_type

    def on_bootstrap(self):
        if self._target_func is not None:
            self._target_func.__globals__.update(self.app.types.biz)
        if self._select_func is not None:
            self._select_func.__globals__.update(self.app.types.biz)
        if self._where_func is not None:
            self._where_func.__globals__.update(self.app.types.biz)
        if self._order_by_func is not None:
            self._order_by_func.__globals__.update(self.app.types.biz)

        self._target_biz_type = self._target_func(MagicMock())

    def execute(self, caller: 'BizObject', *args, **kwargs):
        query = self._build_query(caller, **kwargs)
        result = query.execute(first=self._first)
        return result

    def _build_query(self, caller: 'BizObject', **kwargs) -> 'Query':
        query = (
            pybiz.biz.Query(
                self._target_biz_type,
                alias=self.name,
            ).select(
                self._select_func(caller)
            )
        )
        if self._where_func is not None:
            query.where(self._where_func(caller))
        if self._order_by_func:
            query.order_by(self._order_by_func(caller))
        if self._offset is not None:
            query.offset(self._offset)
        if self._limit is not None:
            query.limit(self._limit)

        return query
