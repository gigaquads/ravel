from functools import reduce
from typing import List, Dict, Set, Text, Type, Tuple

from appyratus.utils import DictUtils, DictObject

from pybiz.util import is_bizobj, is_sequence
from pybiz.constants import IS_BIZOBJ_ANNOTATION

from ..biz_list import BizList
from ..biz_attribute import BizAttribute
from ..relationship import Relationship
from ..view import View, ViewProperty
from .field_property import FieldProperty



class Query(object):
    """
    query = (
        User.select(
            User.account.select(Account.name)
            User.email
        ).where(
            User.age > 14
        ).order_by(
            User.email.desc
        ).limit(1)
    )
    """

    @classmethod
    def from_keys(cls, biz_type: Type['BizObject'], keys: Set[Text]):
        query = cls(biz_type)
        key_tree = DictUtils.unflatten_keys({k: None for k in keys})
        for k, v in key_tree.items():
            field = biz_type.schema.fields.get(k)
            if field:
                query.fields[k] = v
            else:
                attr = getattr(biz_type, k, None)
                if isinstance(attr, Relationship):
                    query.subqueries[k] = cls.from_keys(attr.target_biz_type, v)
                elif isinstance(attr, View):
                    query.views[k] = v
                elif isinstance(attr, BizAttribute):
                    query.attributes[k] = v
        return query

    def __init__(self, biz_type, label=None):
        self.label = label
        self.biz_type = biz_type
        self.fields = {'_id': None}
        self.subqueries = {}
        self.views = {}
        self.attributes = {}
        self._offset = None
        self._limit = None
        self.order_bys = []
        self.predicates = []
        self.executor = Executor()

    def execute(self, first=False):
        targets = self.executor.execute(query=self)
        if first:
            return targets[0] if targets else None
        else:
            return targets

    def select(self, *targets: Tuple) -> 'Query':
        for obj in targets:
            if isinstance(obj, FieldProperty):
                self.fields[obj.field.name] = None
            elif isinstance(obj, ViewProperty):
                self.views[obj.name] = None
            elif isinstance(obj, BizAttribute):
                self.attributes[obj.name] = None
            elif isinstance(obj, Query):
                self.subqueries[obj.label] = obj
        return self
        
    def where(self, *predicates: 'Predicate') -> 'Query':
        self.predicates += predicates
        return self

    def limit(self, limit: int) -> 'Query':
        self._limit = max(limit, 1) if limit is not None else None
        return self

    def offset(self, offset: int) -> 'Query':
        self._offset = max(0, offset) if offset is not None else None
        return self

    def order_by(self, *order_by) -> 'Query':
        if order_by:
            self.order_bys = order_by
        else:
            self.order_bys = None
        return self

    def printf(self, depth=0):
        biz_type_name = self.biz_type.__name__
        chunks = ['SELECT']
        if self.fields:
            fields = ',\n'.join(k for k in self.fields)
            for k in self.fields:
                chunks.append(f' - {k}')
        if self.views:
            views = ',\n'.join(k for k in self.views)
            for k in self.views:
                chunks.append(f' - {k}')
        if self.subqueries:
            for name, subq in self.subqueries.items():
                chunks.append(f' - {name}: (')
                chunks.append('  ' + subq.printf(depth=depth+3))
                chunks.append(f' )')
        chunks.append(f'FROM {biz_type_name}')
        for predicate in self.predicates:
            predicate = reduce(lambda x, y: x & y, self.predicates)
            chunks.append(f'WHERE {predicate}')
        if self.order_bys:
            chunks.append(
                'ORDER_BY ' + ', '.join(f'{x.key} {"DESC" if x.desc else "ASC"}'
                for x in self.order_bys
            ))
        if self._offset is not None:
            chunks.append(f'OFFSET {self._offset}')
        if self._limit:
            chunks.append(f'LIMIT {self._limit}')
        return (f'\n {" " * depth}'.join(chunks))




class Executor(object):
    def execute(self, query: 'Query'):
        dao = query.biz_type.get_dao()
        records = dao.query(
            predicate=reduce(lambda x, y: x & y, query.predicates),
            fields=query.fields, order_by=query.order_bys,
            limit=query.limit, offset=query.offset,
        )
        targets = query.biz_type.BizList(
            query.biz_type(record).clean() for record in records
        )
        return self.execute_recursive(query, targets)

    def execute_recursive(self, query: 'Query', sources: List['BizObject']):
        for k, subquery in query.subqueries.items():
            relationship = query.biz_type.relationships[k]
            targets = relationship.query(sources) # TODO: merge and pass in query.limit and relationship.limit
            self.execute_recursive(subquery, targets)
            for source, target in zip(sources, targets):
                source.related[k] = target
        for k in query.views:
            for source in sources:
                view = getattr(query.biz_type, k)
                view_data = view.query()
                source.viewed[k] = view_data
        for k in query.attributes:
            for source in sources:
                attr = getattr(query.biz_type, k)
                value = attr.query()
                setattr(source, k, value)
        return sources

