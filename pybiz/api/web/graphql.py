from __future__ import absolute_import

from typing import Dict, Set, Text, List, Type

from graphql.parser import GraphQLParser

from pybiz.util import is_bizobj, is_bizlist, is_sequence
from pybiz.biz.internal.query import QuerySpecification
from pybiz.biz import BizObject


class GraphQLEngine(object):
    def __init__(self, document_type: Type[BizObject]):
        self._parser = GraphQLParser()
        self._document_type = document_type

    def query(self, spec: QuerySpecification, dump=False) -> BizObject:
        spec = self.parse(spec) if isinstance(spec, str) else spec
        document = self._document_type()
        self.load(document, spec)
        if dump:
            dumped = document.dump()
            del dumped['id']
            del dumped['rev']
            return dumped
        else:
            return document

    def parse(self, query: Text) -> QuerySpecification:
        def process_ast_node(ast_root, biz_type):
            node_kwargs = {
                arg.name: arg.value for arg in
                getattr(ast_root, 'arguments', ())
            }
            spec = QuerySpecification(
                fields=set(),
                relationships={},
                limit=node_kwargs.pop('limit', None),
                offset=node_kwargs.pop('offset', None),
                kwargs=node_kwargs
            )
            for ast_field in ast_root.selections:
                field_name = ast_field.name
                if field_name in biz_type.schema.fields:
                    spec.fields.add(field_name)
                elif field_name in biz_type.relationships:
                    rel_name = field_name
                    rel = biz_type.relationships[rel_name]
                    rel_spec = process_ast_node(ast_field, rel.target)
                    spec.relationships[rel_name] = rel_spec

            return spec

        ast_root = self._parser.parse(query).definitions[0]
        return process_ast_node(ast_root, self._document_type)

    def load(self, target: BizObject, spec):
        for k, rel_spec in spec.relationships.items():
            rel = target.relationships[k]
            target.related[k] = rel.query(
                target,
                fields=rel_spec.fields,
                limit=rel_spec.limit,
                offset=rel_spec.offset,
                kwargs=rel_spec.kwargs,
            )
            self.load(target.related[k], rel_spec)

        if is_bizobj(target):
            if target._id is not None:
                target.load(spec.fields)
        elif is_bizlist(target):
            target.load(spec.fields)

        return target
