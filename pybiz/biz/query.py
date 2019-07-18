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
            predicate=reduce(lambda x, y: x & y, query.get_predicates()),
            fields=query.target_fields, order_by=query.get_order_by(),
            limit=query.get_limit(), offset=query.get_offset(),
        )
        targets = biz_type.BizList(
            biz_type(record).clean() for record in records
        )
        return self.execute_recursive(query, targets)

    def execute_recursive(self, query: 'Query', sources: List['BizObject']):
        biz_type = query.biz_type
        for k, subquery in query.get_children().items():
            relationship = biz_type.relationships[k]
            # these "where" predicates are AND'ed with the predicates provided
            # by the relationship, not overriding them.
            targets = relationship.query(
                sources,
                select=subquery.target_fields,
                where=subquery.get_predicates(),
                limit=subquery.get_limit(),
                offset=subquery.get_offset(),
                order_by=subquery.get_order_by(),
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
        chunks = [f'FROM {biz_type_name} SELECT']

        target_names = []
        target_names += list(query.target_fields.keys())
        target_names += list(query.target_views.keys())
        target_names += list(query.target_attributes.keys())
        target_names.sort()

        chunks.extend(f' - {k}' for k in target_names)

        if query.get_children():
            for name, subq in sorted(query.get_children().items()):
                chunks.append(f' - {name}: (')
                chunks.append(self.format_query(subq, depth=depth+4))
                chunks.append(f'   )')

        for predicate in (query.get_predicates() or []):
            predicate = reduce(lambda x, y: x & y, query.get_predicates())
            chunks.append(f'WHERE {predicate}')

        if query.get_order_by():
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

        return ('\n'.join(f'{" " * depth}{x}' for x in chunks))


class QueryMarshaller(object):

    schema = QuerySchema()

    def dump(self, query):
        return {
            'alias': query.alias,
            'limit': query.get_limit(),
            'offset': query.get_offset(),
            'order_by': [x.dump() for x in query.get_order_by()],
            'predicates': (
                [x.dump() for x in query.get_predicates()] if
                query.get_predicates() is not None
                else None
            ),
            'children': {
                k: self.dump(v) for k, v in query.get_children().items()
            },
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

        children = []
        for v in data['children'].values():
            child_biz_type_name = v['targets']['biz_type']
            child_biz_type = biz_type.registry.types.biz[child_biz_type_name]
            children.append(self.load(child_biz_type, v))

        targets = children.copy()
        targets += list(data['targets']['fields'].keys())
        targets += list(data['targets']['views'].keys())
        targets += list(data['targets']['attributes'].keys())

        order_by = [OrderBy.load(x) for x in data['order_by']]

        query = Query(biz_type=biz_type, alias=data['alias'])
        query.select(targets)
        query.order_by(order_by)
        query.limit(data['limit'])
        query.offset(data['offset'])

        if data['predicates']:
            query.where([
                Predicate.load(biz_type, x) for x in data['predicates']
            ])

        return query

    @classmethod
    def load_from_keys(cls, biz_type: Type['BizObject'], keys: Set[Text]=None, tree=None) -> 'Query':
        query = Query(biz_type)

        if tree is None:
            assert keys
            tree = DictUtils.unflatten_keys({k: None for k in keys})

        if '*' in tree:
            del tree['*']
            tree.update({
                k: None for k, v in biz_type.schema.fields.items()
                if not v.meta.get('private', False)
            })
        elif not tree:
            tree = {'_id': None, '_rev': None}

        for k, v in tree.items():
            if isinstance(v, dict):
                rel = biz_type.relationships[k]
                subquery = cls.load_from_keys(rel.target_biz_type, tree=v)
                subquery.alias = rel.name
                query.add_target(subquery, None)
            else:
                query.add_target(k, v)
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
    _marshaller = QueryMarshaller()
    _printer  = QueryPrinter()

    def __init__(
        self,
        biz_type: Type['BizType'],
        alias: Text = None,
        fields: Set[Text] = None
    ):
        self._alias = alias
        self._biz_type = biz_type
        self._target_views = {}
        self._target_attributes = {}
        self._children = {}
        self._order_by = []
        self._predicates = None
        self._offset = None
        self._limit = None

        self._target_fields = {'_id': None, '_rev': None}
        if fields:
            self._target_fields.update({k: None for k in fields})
        else:
            self._target_fields.update({
                k: None for k, f in biz_type.schema.fields.items()
                if (f.meta.get('pybiz_is_fk', False) or f.required)
            })

    def execute(self, first=False):
        targets = self._executor.execute(query=self)
        if first:
            return targets[0] if targets else None
        else:
            return targets

    def select(self, *targets: Tuple, append=True) -> 'Query':
        if not append:
            self.clear_targets()
        self.add_targets(targets)
        return self

    def clear_targets(self):
        self._children.clear()
        self._target_fields.clear()
        self._target_views.clear()
        self._target_attributes.clear()

    def add_targets(self, targets):
        for obj in targets:
            if is_sequence(obj):
                self.add_targets(obj)
            elif isinstance(obj, dict):
                for k, v in obj.items():
                    self.add_target(k, v)
            else:
                self.add_target(obj, None)

    def add_target(self, target, params):
        from .internal.relationship_property import RelationshipProperty

        key = None
        targets = None

        try:
            if isinstance(target, str):
                target = getattr(self._biz_type, target)
        except AttributeError:
            raise AttributeError(
                f'{self._biz_type} has no attribute "{target}"'
            )

        if isinstance(target, FieldProperty):
            key = target.field.name
            targets = self._target_fields
        if isinstance(target, RelationshipProperty):
            key = target.relationship.name
            target = Query.from_keys(
                biz_type=target.relationship.target_biz_type,
                keys={'_id', '_rev'}
            )
            targets = self._children
            params = target
        elif isinstance(target, ViewProperty):
            key = target.name
            targets = self._target_views
        elif isinstance(target, BizAttribute):
            key = target.name
            targets = self._target_attributes
        elif isinstance(target, Query):
            key = target.alias
            targets = self._children
            params = target

        if targets is not None:
            targets[key] = params

    def where(self, *predicates: 'Predicate', append=True) -> 'Query':
        if predicates is None:
            self._predicates = None
        else:
            predicates_tmp = []
            for obj in predicates:
                if is_sequence(obj):
                    predicates_tmp.extend(obj)
                else:
                    predicates_tmp.append(obj)
            predicates = tuple(predicates_tmp)
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
        order_by_tmp = []
        for obj in order_by:
            if is_sequence(obj):
                order_by_tmp.extend(obj)
            else:
                order_by_tmp.append(obj)
        order_by = tuple(order_by_tmp)
        self._order_by = order_by if order_by else tuple()
        return self

    def show(self, depth=0):
        self._printer.print_query(query=self, depth=depth)

    def dump(self) -> Dict:
        return self._marshaller.dump(self)

    @classmethod
    def load(cls, biz_type: Type['BizObject'], data: Dict) -> 'Query':
        return cls._marshaller.load(biz_type, data)

    @classmethod
    def from_keys(cls, biz_type: Type['BizObject'], keys: Set[Text]):
        return cls._marshaller.load_from_keys(biz_type, keys=keys)

    @property
    def alias(self) -> Text:
        return self._alias

    @alias.setter
    def alias(self, alias):
        if self._alias is not None:
            raise ValueError('alias is readonly')
        self._alias = alias

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

    def get_predicates(self) -> Dict:
        return self._predicates

    def get_children(self) -> Dict[Text, 'Query']:
        return self._children

    def get_order_by(self) -> Tuple:
        return self._order_by

    def get_limit(self) -> int:
        return self._limit

    def get_offset(self) -> int:
        return self._offset
