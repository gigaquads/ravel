from typing import Dict, Set, Text, List, Type, Tuple

import graphql.parser

from pybiz.util.loggers import console
from pybiz.biz import Query, BizObject

from .graphql_arguments import GraphQLArguments


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
    def _parse_graphql_query_ast(cls, graphql_query_string: Text):
        graphql_doc = cls._ast_parser.parse(graphql_query_string)
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

    def _build_selectors(
        self, target_biz_class: Type[BizObject], ast_node
    ) -> List:
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
