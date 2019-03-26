from __future__ import absolute_import

from typing import Dict, Set, Text, List, Type

from graphql.parser import GraphQLParser

from pybiz.util import is_bizobj, is_bizlist, is_sequence
from pybiz.biz.internal.query import QuerySpecification
from pybiz.biz import BizObject
from pybiz.api.exc import NotAuthorized


class GraphQLEngine(object):
    def __init__(self, document_type: Type[BizObject]):
        self._parser = GraphQLParser()
        self._root_type = document_type

    def query(self, query: Text, context: Dict = None) -> BizObject:
        """
        Execute a GraphQL query by recursively loading the relationships
        declared on an instance of the `document_type` BizObject.
        """
        context = dict(context or {}, authorized=set())
        spec = self.parse(query) if isinstance(query, str) else query
        result = self._load_relationships(self._root_type(), spec, context)
        return result

    def mutate(self, query: Text, context: Dict = None) -> BizObject:
        raise NotImplementedError('not yet supported')

    def parse(self, query: Text) -> QuerySpecification:
        """
        Parse a GraphQL query string to a corresponding QuerySpecification.
        """
        root = self._parser.parse(query).definitions[0]
        return self._build_spec_from_node(root, self._root_type)

    def _load_relationships(self, target: BizObject, spec, context: Dict):
        if target:
            is_authorized = self._authorize(target, context)
            if not is_authorized:
                raise NotAuthorized()

        # load BizObject fields values
        if is_bizobj(target):
            if target._id is not None:
                target.load(spec.fields)
        elif is_bizlist(target):
            for obj in target:
                if obj._id is not None:
                    obj.load(spec.fields)

        # perform depth-first load of target BizObject's relationships
        for k, rel_spec in spec.relationships.items():
            target.related[k] = target.relationships[k].query(
                target,
                fields=rel_spec.fields,
                limit=rel_spec.limit,
                offset=rel_spec.offset,
                kwargs=rel_spec.kwargs,
            )
            self._load_relationships(target.related[k], rel_spec, context)
        return target

    def _authorize(self, target, context):
        objects_to_authorize = []

        if is_bizobj(target):
                objects_to_authorize.append(target)
        elif is_bizlist(target):
            objects_to_authorize.extend(target)

        for obj in objects_to_authorize:
            if isinstance(target, GraphQLObject):
                if obj._id not in context['authorized']:
                    is_authorizd = obj.graphql_authorize(spec, context)
                    if is_authorizd:
                        context['authorized'].add(obj._id)
                    else:
                        return False
        return True

    def _build_spec_from_node(self, root, biz_type):
        node_kwargs = {
            arg.name: arg.value for arg in
            getattr(root, 'arguments', ())
        }
        spec = QuerySpecification(
            fields=set(),
            relationships={},
            limit=node_kwargs.pop('limit', None),
            offset=node_kwargs.pop('offset', None),
            kwargs=node_kwargs
        )
        for ast_field in root.selections:
            field_name = ast_field.name
            if field_name in biz_type.schema.fields:
                spec.fields.add(field_name)
            elif field_name in biz_type.relationships:
                rel_name = field_name
                rel = biz_type.relationships[rel_name]
                rel_spec = self._build_spec_from_node(ast_field, rel.target)
                spec.relationships[rel_name] = rel_spec

        return spec


class GraphQLObject(object):
    def graphql_authorize(self, spec: QuerySpecification, ctx: Dict) -> bool:
        return True
