from typing import Dict

from appyratus.memoize import memoized_property


class QueryRequest(object):
    def __init__(
        self,
        query: 'Query',
        source: 'BizObject' = None,
        resolver: 'Resolver' = None,
        backfiller: 'QueryBackfiller' = None,
        context: Dict = None,
        parent: 'QueryRequest' = None,
        root: 'QueryRequest' = None,
    ):
        self.source_query = query
        self.source = source
        self.backfiller = backfiller
        self.context = context if context is not None else {}
        self.parent = parent
        self.root = root
        self.resolver = resolver

    def __repr__(self):
        return f'Request(query={self._query})'

    @property
    def params(self):
        return self._query.params

    @memoized_property
    def query(self) -> 'Query':
        query = self.source_query.biz_class.select()
        query.configure(self.source_query.options)
        query.select(self.source_query.params.select)
        query.where(self.source_query.params.where)
        query.order_by(self.source_query.params.order_by)
        query.limit(self.source_query.params.limit)
        query.offset(self.source_query.params.offset)
        return query
