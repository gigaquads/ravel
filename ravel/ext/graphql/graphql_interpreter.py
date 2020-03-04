from typing import Dict, Set, Text, List, Type, Tuple

import graphql.ast
import graphql.parser

from appyratus.utils import DictObject

from ravel.util.loggers import console
from ravel.resource import Resource
from ravel.query.query import Query
from ravel.query.request import Request

from .graphql_arguments import GraphQLArguments
from .graphql_result import GraphQLResult


class GraphQLInterpreter(object):
    """
    The purpose of the GraphQLInterpreter is to take a a raw GraphQL query
    string and generate a corresponding ravel Query object.
    """

    def __init__(self, root_resource_type: Type['Resource'], persist=False):
        if not issubclass(root_resource_type, GraphQLResult):
            raise ValueError(
                'root resource type must be a GraphQLResult subclass'
            )
        self._persist = persist
        self._root_resource_type = root_resource_type
        self._ast_parser = graphql.parser.GraphQLParser()

    def interpret(self, graphql_query_string: Text, context=None) -> Query:
        context = context or DictObject()
        root_node = self._parse_graphql_query_ast(graphql_query_string)
        query = self._build_query(root_node, self._root_resource_type, context)

        result = query.execute(first=True)
        result.graphql_query = graphql_query_string

        if self._persist:
            result.create()

        return result

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
                request = self._build_request(child_node, resolver)
                query.select(request)

        return query

    def _build_request(self, node, resolver):
        schema = resolver.owner.ravel.schema
        resolvers = resolver.owner.ravel.resolvers

        name = node.name
        request = Request(resolver)
        args = GraphQLArguments.parse(resolver.target, node)

        for child_node in node.selections:
            name = child_node.name
            if name not in resolvers:
                raise Exception(f'unrecognized resolver: {name}')

            if name in schema.fields:
                request.select(name)
            else:
                child_resolver = resolvers[name]
                child_request = self._build_request(child_node, child_resolver)

                if args.where:
                    request.order_by(args.where)
                if args.order_by:
                    request.order_by(args.order_by)
                if args.offset:
                    request.order_by(args.offset)
                if args.limit:
                    request.order_by(args.limit)
                if args.custom:
                    query.parameters.update(args.custom)

                request.select(child_request)

        return request
