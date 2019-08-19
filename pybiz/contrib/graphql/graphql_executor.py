from typing import Dict, Set, Text, List, Type, Tuple

from graphql.parser import GraphQLParser

import pybiz.biz

from .graphql_arguments import GraphQLArguments


class GraphQLExecutor(object):

    def __init__(self, root_biz_class: Type['BizType']):
        self._graphql_parser = GraphQLParser()
        self._root_biz_class = root_biz_class

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

    def _build_pybiz_query(self, node, biz_class=None):
        biz_class = biz_class or self._root_biz_class
        args = GraphQLArguments.parse(biz_class, node)

        selected_attributes = []
        selected_subqueries = []
        for child_node in node.selections:
            child_name = child_node.name
            relationships = biz_class.attributes.by_category('relationship')
            biz_attr = biz_class.attributes.by_name(child_name)
            if biz_attr is None:
                continue
            if biz_attr.category == 'graphql_selector':
                child_query = self._build_pybiz_query(child_node, biz_attr.target_biz_class)
                selected_subqueries.append(child_query)
            elif biz_attr.category == 'relationship':
                rel = relationships.get(child_name)
                child_query = self._build_pybiz_query(child_node, rel.target_biz_class)
                selected_subqueries.append(child_query)
            elif child_name in biz_class.attributes:
                selected_attributes.append(child_name)

        return (
            pybiz.biz.Query(
                biz_class, alias=node.name
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
