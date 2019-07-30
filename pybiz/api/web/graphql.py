from __future__ import absolute_import

import re

from typing import Dict, Set, Text, List, Type, Tuple

from graphql.parser import GraphQLParser

from pybiz.biz import Query
from pybiz.biz.order_by import OrderBy
from pybiz.api.exc import NotAuthorized
from pybiz.predicate import Predicate, PredicateParser

RE_ORDER_BY = re.compile(r'(\w+)\s+((?:desc)|(?:asc))', re.I)


class GraphQLArguments(object):

    @classmethod
    def parse(cls, biz_type, node, predicate_parser) -> 'GraphQLArguments':
        args = {
            arg.name: arg.value for arg in
            getattr(node, 'arguments', ())
        }
        return cls(
            where=cls._parse_where(
                biz_type, args.pop('where', None), predicate_parser
            ),
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
        biz_type: Type['BizObject'],
        predicate_strings: List[Text],
        parser: PredicateParser
    ) -> List['Predicate']:
        if isinstance(predicate_strings, str):
            predicate_strings = [predicate_strings]
        return [
            parser.parse(biz_type, pred_str)
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


class GraphQLExecutor(object):
    def __init__(self, root_biz_type: Type['BizType']):
        self._graphql_parser = GraphQLParser()
        self._root_biz_type = root_biz_type
        self._predicate_parser = PredicateParser()

    def query(
        self,
        graphql_query_string: Text,
        context: Dict = None,
        execute: bool = False,
    ):
        graphql_query = self._parse_graphql_query(graphql_query_string)
        pybiz_query = self._build_pybiz_query(graphql_query)
        if execute:
            return pybiz_query.execute()
        else:
            return pybiz_query

    def _parse_graphql_query(self, graphql_query_string: Text):
        graphql_doc = self._graphql_parser.parse(graphql_query_string)
        graphql_query = graphql_doc.definitions[0]
        return graphql_query

    def _build_pybiz_query(self, node, biz_type=None):
        biz_type = biz_type or self._root_biz_type
        args = GraphQLArguments.parse(biz_type, node, self._predicate_parser)

        selected_attributes = []
        selected_subqueries = []
        for child_node in node.selections:
            child_name = child_node.name
            if child_name in biz_type.relationships:
                rel = biz_type.relationships.get(child_name)
                child_query = self._build_pybiz_query(child_node, rel.target_biz_type)
                selected_subqueries.append(child_query)
            elif child_name in biz_type.selectable_attribute_names:
                selected_attributes.append(child_name)

        return (
            Query(
                biz_type, alias=node.name
            )
            .select(
                selected_attributes,
                selected_subqueries
            )
            .where(args.where)
            .order_by(args.order_by)
            .offset(args.offset)
            .limit(args.limit)
        )
