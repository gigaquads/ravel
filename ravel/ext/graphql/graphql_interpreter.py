from typing import Dict, Set, Text, List, Type, Tuple

import graphql.ast
import graphql.parser

from appyratus.utils.dict_utils import DictObject

from ravel.util.loggers import console
from ravel.resource import Resource
from ravel.query.query import Query
from ravel.query.request import Request

from .graphql_arguments import GraphqlArguments
from .graphql_query import GraphqlQuery


class GraphqlInterpreter(object):
    """
    The purpose of the GraphQlInterpreter is to take a a raw GraphQL query
    string and generate a corresponding ravel Query object.
    """

    def __init__(self, root_resource_type: Type['Resource']):
        if not issubclass(root_resource_type, GraphqlQuery):
            raise ValueError(
                'root resource type must be a GraphqlQuery subclass'
            )
        self._root_resource_type = root_resource_type
        self._ast_parser = graphql.parser.GraphQLParser()

    def interpret(self, graphql_query_string: Text, context=None) -> Query:
        context = context or DictObject()
        root_node = self._parse_graphql_query_ast(graphql_query_string)
        root_node.source = graphql_query_string
        return self._build_query(root_node, self._root_resource_type, context)

    def _parse_graphql_query_ast(self, graphql_query_string: Text):
        graphql_doc = self._ast_parser.parse(graphql_query_string)
        graphql_query = graphql_doc.definitions[0]
        return graphql_query

    def _build_query(
        self,
        node: graphql.ast.Field,
        target_resource_type: Resource,
        context: Dict,
    ) -> Query:
        """
        Recursively build a ravel Query object from the given low-level GraphQL
        AST node returned from the core GraphQL language parser.
        """
        result = self._root_resource_type()
        query = Query(target=self._root_resource_type, sources=[result])

        for child_node in node.selections:
            resolver = result.ravel.resolvers.get(child_node.name)
            if resolver is None:
                raise Exception(f'unrecognized resolver: {child_node.name}')
            else:
                request = self._build_request(child_node, query, resolver)
                query.select(request)

        return query.limit(1)

    def _build_request(self, node, query, resolver):
        schema = resolver.owner.ravel.schema
        resolvers = resolver.target.ravel.resolvers

        name = node.name
        request = Request(resolver, query=query)
        args = GraphqlArguments.parse(resolver.target, node)

        if args.where:
            request.where(args.where)
        if args.order_by:
            request.order_by(args.order_by)
        if args.offset:
            request.offset(args.offset)
        if args.limit:
            request.limit(args.limit)
        if args.custom:
            request.parameters.update(args.custom)

        for child_node in node.selections:
            name = child_node.name
            if name not in resolvers:
                raise Exception(f'unrecognized resolver: {name}')

            if name in schema.fields:
                request.select(name)
            else:
                child_resolver = resolvers[name]
                child_request = self._build_request(
                    child_node, query, child_resolver
                )

                request.select(child_request)

        return request
