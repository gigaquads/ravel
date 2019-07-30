from typing import Dict, Set, Text, List, Type, Tuple

from graphql.parser import GraphQLParser

from pybiz.biz import Query
from pybiz.predicate import PredicateParser

from .graphql_arguments import GraphQLArguments


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
