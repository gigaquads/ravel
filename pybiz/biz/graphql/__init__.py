import re
import threading

from typing import Dict, Set, Text, List, Type, Tuple

import graphql.parser

from pybiz.util.loggers import console
from pybiz.predicate import PredicateParser
from pybiz.biz import (
    OrderBy, Query, QueryExecutor, QueryBackfiller, BizAttribute, BizObject
)


class GraphQLQueryParser(object):
    """
    The purpose of the GraphQLQueryParser is to translate or "parse" a raw
    GraphQL query string into a corresponding pybiz Query object.
    """

    def __init__(self, root_biz_class: Type[BizObject]):
        self._root_biz_class = root_biz_class
        self._ast_parser = graphql.parser.GraphQLParser()

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
        """
        Recursively build a pybiz Query object from the given low-level GraphQL
        AST node returned from the core GraphQL language parser.
        """
        target_biz_class = target_biz_class or self._root_biz_class
        args = GraphQLArguments.parse(target_biz_class, ast_node)
        selectors = self._build_selectors(target_biz_class, ast_node)
        return Query(
            biz_class=target_biz_class,
            alias=ast_node.name
            select=selectors,
            where=args.where,
            order_by=args.order_by,
            limit=args.limit,
            offset=args.offset,
        )

    def _build_selectors(self, target_biz_class, ast_node):
        """
        Build the selectors to be passed into the query's `select` method.
        """
        selectors = []
        for child_ast_node in ast_node.selections:
            child_name = child_ast_node.name
            if child_name in target_biz_class.schema.fields:
                selectors.append(child_name)
            elif child_name in target_biz_class.attributes:
                # TODO: support arguments for BizAttributeQueries
                selectors.append(child_name)
            elif child_name in target_biz_class.relationships:
                # recursively build query based on relationship
                rel = target_biz_class.relationships[child_name]
                child_biz_class = rel.target_biz_class
                child_query = self._build_pybiz_query(
                    child_ast_node, target_biz_class=child_biz_class
                )
                selectors.append(child_query)
            else:
                console.warn(
                    f'unknown field {target_biz_class.__name__}.{child_name} '
                    f'selected in GraphQL query'
                )
        return selectors


class GraphQLArguments(object):
    """
    This class is responsible for parsing and normalizing the base arguments
    supplied to a GraphQL query node into the corresponding arguments expected
    by a pybiz Query object.
    """

    _re_order_by = re.compile(r'(\w+)\s+((?:desc)|(?:asc))', re.I)

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
            custom=args  # `custom` is whatever remains in args
        )

    @classmethod
    def _parse_order_by(cls, order_by_strs: Tuple[Text]) -> Tuple[OrderBy]:
        order_by_strs = order_by_strs or []
        order_by_list = []
        for order_by_str in order_by_strs:
            match = self._re_order_by.match(order_by_str)
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

    def __init__(self, where, order_by, offset, limit, custom: Dict):
        self.where = where
        self.order_by = order_by
        self.offset = offset
        self.limit = limit
        self.custom = custom

    def __getattr__(self, attr):
        return self.custom.get(attr)
