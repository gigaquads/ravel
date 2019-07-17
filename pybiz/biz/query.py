from functools import reduce
from typing import List, Dict, Set, Text, Type, Tuple

from appyratus.utils import DictUtils, DictObject

from pybiz.util import is_bizobj, is_sequence
from pybiz.constants import IS_BIZOBJ_ANNOTATION
from pybiz.schema import Schema, fields
from pybiz.predicate import Predicate

from .internal.field_property import FieldProperty
from .internal.order_by import OrderBy
from .biz_list import BizList
from .biz_attribute import BizAttribute
from .relationship import Relationship
from .view import View, ViewProperty


class QuerySchema(Schema):
    alias = fields.String()
    limit = fields.Int(nullable=True)
    offset = fields.Int(nullable=True)
    order_by = fields.List(fields.String(), default=[])
    children = fields.Dict(default={})
    predicates = fields.List(fields.Dict(), nullable=True)
    targets = fields.Nested({
        'biz_type': fields.String(),
        'attributes': fields.Dict(default={}),
        'fields': fields.Dict(default={}),
        'views': fields.Dict(default={}),
    })


class QueryExecutor(object):
    def execute(self, query: 'Query'):
        biz_type = query.biz_type
        dao = biz_type.get_dao()
        records = dao.query(
            predicate=reduce(lambda x, y: x & y, query.predicates),
            fields=query.target_fields, order_by=query.get_order_by(),
            limit=query.get_limit(), offset=query.get_offset(),
        )
        targets = biz_type.BizList(
            biz_type(record).clean() for record in records
        )
        return self.execute_recursive(query, targets)

    def execute_recursive(self, query: 'Query', sources: List['BizObject']):
        biz_type = query.biz_type
        for k, subquery in query.children.items():
            relationship = biz_type.relationships[k]
            targets = relationship.query(
                sources,
                limit=query.get_limit(),
                offset=query.get_offset(),
                order_by=query.get_order_by(),
            )
            self.execute_recursive(subquery, targets)
            for source, target in zip(sources, targets):
                source.related[k] = target
        for k in query.target_views:
            for source in sources:
                view = getattr(biz_type, k)
                view_data = view.query()
                source.viewed[k] = view_data
        for k in query.target_attributes:
            for source in sources:
                attr = getattr(biz_type, k)
                value = attr.query()
                setattr(source, k, value)
        return sources


class QueryPrinter(object):
    def print_query(self, query, depth=0):
        print(self.format_query(query, depth=depth))

    def format_query(self, query, depth=0) -> Text:
        biz_type_name = query.biz_type.__name__
        chunks = ['SELECT']

        if query.target_fields:
            for k in query.target_fields:
                chunks.append(f' - {k}')

        if query.target_views:
            for k in query.target_views:
                chunks.append(f' - {k}')

        if query.children:
            for name, subq in query.children.items():
                chunks.append(f' - {name}: (')
                chunks.append('  ' + subq.show(depth=depth+3))
                chunks.append(f' )')

        chunks.append(f'FROM {biz_type_name}')

        for predicate in query.predicates:
            predicate = reduce(lambda x, y: x & y, query.predicates)
            chunks.append(f'WHERE {predicate}')

        chunks.append(
            'ORDER_BY ' + ', '.join(
            f'{x.key} {"DESC" if x.desc else "ASC"}'
            for x in query.get_order_by()
        ))

        offset = query.get_offset()
        if offset is not None:
            chunks.append(f'OFFSET {offset}')

        limit = query.get_limit()
        if limit is not None:
            chunks.append(f'LIMIT {limit}')

        return (f'\n {" " * depth}'.join(chunks))


class QueryDumper(object):

    schema = QuerySchema()

    def dump(self, query):
        return {
            'alias': query.alias,
            'limit': query.get_limit(),
            'offset': query.get_offset(),
            'order_by': [x.dump() for x in query.get_order_by()],
            'predicates': (
                [x.dump() for x in query.predicates] if
                query.predicates is not None
                else None
            ),
            'children': {k: self.dump(v) for k, v in query.children.items()},
            'targets': {
                'biz_type': query.biz_type.__name__,
                'attributes': query.target_attributes,
                'fields': query.target_fields,
                'views': query.target_views,
            }
        }

    def load(self, biz_type, data):
        data, errors = self.schema.process(data)
        if errors:
            # TODO: raise custom exceptions
            raise ValueError(str(errors))

        query = Query(biz_type, alias=data['alias'])
        query.select(*list(data['targets']['fields'].keys()))
        query.select(*list(data['targets']['views'].keys()))
        query.select(*list(data['targets']['attributes'].keys()))
        query.select(*[
            self.load(
                biz_type.registry.types.biz[x['targets']['biz_type']], x
            ) for x in data['children'].values()
        ])
        if data['predicates']:
            query.where(*[
                Predicate.load(biz_type, x) for x in data['predicates']
            ])
        query.order_by(*[
            OrderBy.load(x) for x in data['order_by']
        ])
        query.limit(data['limit'])
        query.offset(data['offset'])

        return query

    @classmethod
    def load_from_keys(cls, biz_type: Type['BizObject'], keys: Set[Text]) -> 'Query':
        query = Query(biz_type)
        key_tree = DictUtils.unflatten_keys({k: None for k in keys})

        for k, v in key_tree.items():
            field = biz_type.schema.fields.get(k)
            if field:
                query.fields[k] = v
            else:
                attr = getattr(biz_type, k, None)
                if isinstance(attr, Relationship):
                    query._children[k] = cls.from_keys(attr.target_biz_type, v)
                elif isinstance(attr, View):
                    query.views[k] = v
                elif isinstance(attr, BizAttribute):
                    query.attributes[k] = v

        return query


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

    _executor = QueryExecutor()
    _printer  = QueryPrinter()
    _dumper = QueryDumper()

    def __init__(self, biz_type: Type['BizType'], alias: Text = None):
        self._alias = alias
        self._biz_type = biz_type
        self._target_fields = {'_id': None}
        self._target_views = {}
        self._target_attributes = {}
        self._children = {}
        self._order_by = []
        self._predicates = None
        self._offset = None
        self._limit = None

    def execute(self, first=False):
        targets = self._executor.execute(query=self)
        if first:
            return targets[0] if targets else None
        else:
            return targets

    def select(self, *targets: Tuple, append=True) -> 'Query':
        if not append:
            self._children.clear()
            self._target_fields.clear()
            self._target_views.clear()

        for obj in targets:
            if isinstance(obj, str):
                obj = getattr(self._biz_type, obj)
            if isinstance(obj, FieldProperty):
                self._target_fields[obj.field.name] = None
            elif isinstance(obj, ViewProperty):
                self._target_views[obj.name] = None
            elif isinstance(obj, BizAttribute):
                self._target_attributes[obj.name] = None
            elif isinstance(obj, Query):
                self._children[obj.alias] = obj

        return self

    def where(self, *predicates: 'Predicate', append=True) -> 'Query':
        if predicates is None:
            self._predicates = None
        else:
            if self._predicates is None:
                self._predicates = tuple()
            if append:
                self._predicates += predicates
            else:
                self._predicates = predicates
        return self

    def limit(self, limit: int) -> 'Query':
        self._limit = max(limit, 1) if limit is not None else None
        return self

    def offset(self, offset: int) -> 'Query':
        self._offset = max(0, offset) if offset is not None else None
        return self

    def order_by(self, *order_by) -> 'Query':
        self._order_by = order_by if order_by else tuple()
        return self

    def show(self, depth=0):
        self._printer.print_query(query=self, depth=depth)

    def dump(self) -> Dict:
        return self._dumper.dump(self)

    @classmethod
    def load(cls, biz_type: Type['BizObject'], data: Dict) -> 'Query':
        return cls._dumper.load(biz_type, data)

    @property
    def alias(self) -> Text:
        return self._alias

    @property
    def biz_type(self) -> Type['BizObject']:
        return self._biz_type

    @property
    def target_fields(self) -> Dict:
        return self._target_fields
    @property
    def target_views(self) -> Dict:
        return self._target_views

    @property
    def target_attributes(self) -> Dict:
        return self._target_attributes

    @property
    def predicates(self) -> Dict:
        return self._predicates

    @property
    def children(self) -> Dict[Text, 'Query']:
        return self._children

    def get_order_by(self) -> Tuple:
        return self._order_by

    def get_limit(self) -> int:
        return self._limit

    def get_offset(self) -> int:
        return self._offset
