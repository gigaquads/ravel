from typing import Dict, Set, Text, List, Type, Tuple

import graphql.ast
import graphql.parser

from pybiz.util.loggers import console
#from pybiz.biz import Query, BizObject, BizAttributeQuery, FieldPropertyQuery

from .graphql_arguments import GraphQLArguments


class GraphQLInterpreter(object):
    """
    The purpose of the GraphQLInterpreter is to take a a raw GraphQL query
    string and generate a corresponding pybiz Query object.
    """

    def __init__(self, root_biz_class: Type['BizObject']):
        self._root_biz_class = root_biz_class
        self._ast_parser = graphql.parser.GraphQLParser()

#    def interpret(self, graphql_query_string: Text) -> Query:
#        context = {}
#        root_ast_node = self._parse_graphql_query_ast(graphql_query_string)
#        return self._build_query(root_ast_node, self._root_biz_class, context)
#
#    def _parse_graphql_query_ast(self, graphql_query_string: Text):
#        graphql_doc = self._ast_parser.parse(graphql_query_string)
#        graphql_query = graphql_doc.definitions[0]
#        return graphql_query
#
#    def _build_subqueries(
#        self,
#        target_biz_class: Type[BizObject],
#        ast_node,
#        context: Dict,
#    ) -> List:
#        """
#        Build the selectors to be passed into the query's `select` method.
#        """
#        selectors = []
#        for child_ast_node in ast_node.selections:
#            child_name = child_ast_node.name
#            if child_name in target_biz_class.Schema.fields:
#                fprop_query = self._build_pybiz_field_property_query(
#                    child_ast_node, target_biz_class, context
#                )
#                selectors.append(fprop_query)
#            elif child_name in target_biz_class.pybiz.attributes.relationships:
#                # Recursively build query based on relationship
#                #
#                # NOTE: this check MUST be performed BEFORE the check for each
#                # child_name in target_biz_class.pybiz.attributes
#                relationships = target_biz_class.pybiz.attributes.relationships
#                rel = relationships[child_name]
#                child_biz_class = rel.target_biz_class
#                child_query = self._build_query(
#                    child_ast_node, child_biz_class, context
#                )
#                selectors.append(child_query)
#            elif child_name in target_biz_class.pybiz.attributes:
#                biz_attr_query = self._build_pybiz_biz_attr_query(
#                    child_ast_node, target_biz_class, context
#                )
#                selectors.append(biz_attr_query)
#            else:
#                console.warning(
#                    f'unknown field {target_biz_class.__name__}.{child_name} '
#                    f'selected in GraphQL query'
#                )
#        return selectors
#
#    def _build_query(
#        self,
#        ast_node: graphql.ast.Field,
#        target_biz_class: BizObject,
#        context: Dict,
#    ) -> Query:
#        """
#        Recursively build a pybiz Query object from the given low-level GraphQL
#        AST node returned from the core GraphQL language parser.
#        """
#        args = GraphQLArguments.parse(target_biz_class, ast_node)
#        selectors = self._build_subqueries(target_biz_class, ast_node, context)
#        query = Query(
#            biz_class=target_biz_class, alias=ast_node.name, context=context,
#            select=selectors, where=args.where, order_by=args.order_by,
#            limit=args.limit, offset=args.offset,
#        )
#        query.params.custom = args.custom
#        return query
#
#    def _build_pybiz_biz_attr_query(
#        self,
#        ast_node: graphql.ast.Field,
#        target_biz_class: BizObject,
#        context: Dict,
#    ) -> BizAttributeQuery:
#        """
#        """
#        alias = ast_node.name
#        biz_attr = target_biz_class.pybiz.attributes.by_name(ast_node.name)
#        params = GraphQLArguments.extract_arguments_dict(ast_node)
#        return BizAttributeQuery(
#            biz_attr, alias=alias, params=params, context=context
#        )
#
#    def _build_pybiz_field_property_query(
#        self,
#        ast_node: graphql.ast.Field,
#        target_biz_class: BizObject,
#        context: Dict,
#    ) -> FieldPropertyQuery:
#        alias = ast_node.name
#        fprop = getattr(target_biz_class, ast_node.name)
#        params = GraphQLArguments.extract_arguments_dict(ast_node)
#        return FieldPropertyQuery(
#            fprop, alias=alias, params=params, context=context
#        )
