from typing import Text, Dict, Callable, Tuple

from appyratus.schema import Schema

from pybiz.util import normalize_to_tuple

from .biz_attribute import BizAttribute
from .query import Query


class Subquery(BizAttribute):
    def __init__(
        self,
        target: Callable,
        select: Callable,
        where: Callable,
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

    @property
    def query_execution_order_key(self):
        return 100

    @property
    def biz_attr_name(self):
        return 'subquery'

    def on_bootstrap(self):
        if self._target_func is not None:
            self._target_func.__globals__.update(self.registry.types.biz)
        if self._select_func is not None:
            self._select_func.__globals__.update(self.registry.types.biz)
        if self._where_func is not None:
            self._where_func.__globals__.update(self.registry.types.biz)
        if self._order_by_func is not None:
            self._order_by_func.__globals__.update(self.registry.types.biz)

    def execute(self, caller: 'BizObject', *args, **kwargs):
        query = self._build_query(caller)
        result = query.execute(first=self._first)
        return result

    def _build_query(self, caller: 'BizObject') -> 'Query':
        query = (
            Query(
                biz_type=self._target_func(caller),
                alias=self.name,
            ).select(
                self._select_func(caller)
            ).where(
                self._where_func(caller)
            )
        )
        if self._order_by_func:
            query.order_by(self._order_by_func(caller))
        if self._offset is not None:
            query.offset(self._offset)
        if self._limit is not None:
            query.limit(self._limit)

        return query
