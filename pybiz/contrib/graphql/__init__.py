import re
import threading

from typing import Dict, Set, Text, List, Type, Tuple

from graphql.parser import GraphQLParser

from pybiz.util.loggers import console
from pybiz.predicate import PredicateParser
from pybiz.biz import (
    OrderBy, Query, QueryExecutor, QueryBackfiller, BizAttribute, BizObject
)

RE_ORDER_BY = re.compile(r'(\w+)\s+((?:desc)|(?:asc))', re.I)


class Executor(object):
    def __init__(self, root_biz_class: Type[BizObject]):
        self.parser = Parser(root_biz_class)

    def query(self, graphql_query: Text) -> Query:
        return self.parser.parse(graphql_query)


class Parser(object):

    _ast_parser = GraphQLParser()

    def __init__(self, root_biz_class: Type[BizObject]):
        self.root_biz_class = root_biz_class

    def parse(self, graphql_query_string: Text) -> Query:
        graphql_query = self._parse_graphql_query_ast(graphql_query_string)
        pybiz_query = self._build_pybiz_query(graphql_query)
        return pybiz_query

    @classmethod
    def _parse_graphql_query_ast(cls, query_string: Text):
        graphql_doc = cls._ast_parser.parse(query_string)
        graphql_query = graphql_doc.definitions[0]
        return graphql_query

    def _build_pybiz_query(self, ast_node, target_biz_class=None) -> Query:
        selected_names = {u.name for u in ast_node.selections}
        target_biz_class = target_biz_class or self.root_biz_class
        query = Query(biz_class=target_biz_class, alias=ast_node.name)

        for child_ast_node in ast_node.selections:
            child_name = child_ast_node.name

            if child_name in target_biz_class.schema.fields:
                query.select(child_name)
            elif child_name in target_biz_class.relationships:
                rel = target_biz_class.relationships[child_name]
                child_biz_class = rel.target_biz_class
                child_query = self._build_pybiz_query(
                    child_ast_node, target_biz_class=child_biz_class
                )
                query.select(child_query)
            elif child_name in target_biz_class.attributes:
                query.select(child_name)
            else:
                console.warn(
                    f'unknown field {target_biz_class.__name__}.{child_name} '
                    f'selected in GraphQL query'
                )

        # read in raw arguments supplied to the GraphQL node, preparing
        # them to be passed into the pybiz Query object.
        graphql_args = GraphQLArguments.parse(target_biz_class, ast_node)

        query.where(graphql_args.where)
        query.order_by(graphql_args.order_by)
        query.offset(graphql_args.offset)
        query.limit(graphql_args.limit)

        return query



class GraphqlQueryTarget(BizAttribute):
    def __init__(
        self,
        target: Type['BizObject'],
        schema: 'GraphqlSchema' = None,
        many: bool = True,
        *args,
        **kwargs
    ):
        super().__init__(*args, **kwargs)
        self._target_biz_class = target
        self._many = many
        self._schema = schema

    def on_bootstrap(self):
        if self._schema is None:
            self._schema = GraphqlSchema.build(
                self.name, self._target_biz_class
            )

    @property
    def category(self):
        return 'graphql_query_target'

    @property
    def target_biz_class(self) -> Type['BizObject']:
        return self._target_biz_class

    @property
    def graphql_schema(self) -> 'GraphqlSchema':
        return self._schema

    @property
    def many(self) -> bool:
        return self._many


class GraphqlExecutor(object):
    """
    """
    def __init__(self, biz_class):
        self._ast_parser = GraphQLParser()
        self._schema = GraphqlSchema.build(None, biz_class)

    def query(self, graphql_query_string: Text):
        return self._schema.query(graphql_query_string)


class GraphqlSchema(object):
    def __init__(self, name, biz_class):
        self.name = name
        self.biz_class = biz_class
        self.fields = {}
        self.children = {}
        self.biz_attributes = {}
        self.query_parser = GraphqlQueryParser()

    @classmethod
    def build(cls, name, biz_class, memoized=None):
        memoized = memoized if memoized is not None else {}
        if biz_class in memoized:
            return memoized[biz_class]

        schema = memoized[biz_class] = cls(name, biz_class)
        schema.fields = biz_class.schema.fields.copy()
        schema.children = {
            r.name: cls.build(
                r.name, r.target_biz_class, memoized=memoized
            )
            for r in biz_class.relationships.values()
        }
        schema.children.update({
            k: cls.build(k, biz_attr.target_biz_class, memoized=memoized)
            for k, biz_attr in biz_class.attributes.by_category('graphql_query_target').items()
        })
        schema.biz_attributes = {
            k: v for k, v in biz_class.attributes.items()
            if k not in schema.children
        }

        return schema

    def query(self, source: Text) -> 'Query':
        return self.query_parser.parse(self, source)


class GraphqlQueryParser(object):

    _ast_parser = GraphQLParser()

    def parse(self, schema, source: Text) -> 'Query':
        graphql_ast = self.parse_graphql_query_string(source)
        return self.build_pybiz_query(schema, graphql_ast)

    @classmethod
    def parse_graphql_query_string(cls, query_string: Text):
        graphql_doc = cls._ast_parser.parse(query_string)
        graphql_query = graphql_doc.definitions[0]
        return graphql_query

    @classmethod
    def build_pybiz_query(cls, schema, ast_node):
        query = Query(
            biz_class=schema.biz_class,
            alias=ast_node.name,
            callbacks=[cls._mask_keys]
        )

        selected_keys = set()
        for child_ast_node in ast_node.selections:
            child_name = child_ast_node.name
            if child_name in schema.fields:
                query.select(child_name)
            elif child_name in schema.biz_attributes:
                query.select(child_name)
            elif child_name in schema.children:
                child_schema = schema.children[child_name]
                child_query = cls.build_pybiz_query(
                    child_schema, child_ast_node
                )
                query.select(child_query)
            else:
                continue
            selected_keys.add(child_name)

        graphql_args = GraphQLArguments.parse(schema.biz_class, ast_node)

        query.where(graphql_args.where)
        query.order_by(graphql_args.order_by)
        query.offset(graphql_args.offset)
        query.limit(graphql_args.limit)

        return query

    @classmethod
    def _mask_keys(cls, query, biz_thing):
        masked_field_names = (
            query.biz_class.schema.fields.keys() - query.params.keys()
        )
        biz_thing.unload(masked_field_names)


class GraphQLArguments(object):

    _thread_local = threading.local()
    _thread_local.predicate_parser = PredicateParser()

    @classmethod
    def parse(cls, biz_class, node) -> 'GraphQLArguments':
        args = {
            arg.name: arg.value for arg in
            getattr(node, 'arguments', ())
        }
        return cls(
            where=cls._parse_where(biz_class, args.pop('where', None)),
            order_by=cls._parse_order_by(args.pop('order_by', None)),
            offset=cls._parse_offset(args.pop('offset', None)),
            limit=cls._parse_limit(args.pop('limit', None)),
            custom=args
        )

    @classmethod
    def _parse_order_by(cls, order_by_strs: Tuple[Text]) -> Tuple[OrderBy]:
        order_by_strs = order_by_strs or []
        order_by_list = []
        for order_by_str in order_by_strs:
            match = RE_ORDER_BY.match(order_by_str)
            if match is not None:
                key, asc_or_desc = match.groups()
                order_by = OrderBy.load({
                    'desc': asc_or_desc.lower() == 'desc',
                    'key': key,
                })
                order_by_list.append(order_by)
        return tuple(order_by_list)

    @classmethod
    def _parse_offset(cls, raw_offset) -> int:
        offset = None
        if raw_offset is not None:
            offset = max(int(raw_offset), 0)
        return offset

    @classmethod
    def _parse_limit(cls, raw_limit) -> int:
        limit = None
        if raw_limit is not None:
            limit = max(int(raw_limit), 1)
        return limit

    @classmethod
    def _parse_where(
        cls,
        biz_class: Type['BizObject'],
        predicate_strings: List[Text],
    ) -> List['Predicate']:
        parser = cls._thread_local.predicate_parser
        if isinstance(predicate_strings, str):
            predicate_strings = [predicate_strings]
        return [
            parser.parse(biz_class, pred_str)
            for pred_str in (predicate_strings or [])
        ]

    def __init__(self, where, order_by, offset, limit, custom):
        self.where = where
        self.order_by = order_by
        self.offset = offset
        self.limit = limit
        self.custom = custom

    def __getattr__(self, attr):
        return self.custom.get(attr)
