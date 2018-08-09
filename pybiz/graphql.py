from __future__ import absolute_import

from typing import Dict

from graphql.parser import GraphQLParser

from pybiz.util import is_bizobj


class GraphQLNode(object):
    pass


class GraphQLFieldNode(GraphQLNode):

    def __init__(self, relationship, ast_field, parent):
        self.relationship = relationship
        self.parent = parent
        self.name = ast_field.name
        self.alias = getattr(ast_field, 'alias', None)
        self.value = getattr(ast_field, 'value', None)
        self.relationships = {}
        self.fields = {}
        self.context = {}

        # these are kwargs to pass into the get method of
        # the related bizobj class (if there is one)
        self.args = {
            arg.name: arg.value for arg in
            getattr(ast_field, 'arguments', ())
            }

        # initialize nested relationships and fields
        for child_ast_field in ast_field.selections:
            child_name = child_ast_field.name
            rel = self.bizobj_class.relationships.get(child_name)
            if rel is not None:
                child = GraphQLFieldNode(rel, child_ast_field, self)
                self.relationships[child_name] = child
            else:
                child = GraphQLFieldNode(None, child_ast_field, self)
                self.fields[child_name] = child

    @property
    def key(self):
        return self.alias or self.name

    @property
    def field_names(self):
        return set(self.fields.keys())

    @property
    def bizobj_class(self):
        return self.relationship.bizobj_class

    def execute(self, func):
        """
        Execute a function with the following signature on this field
        as well as its child fields.

        ```python3
        def execute(field) -> dict:
            pass
        ```
        """
        results = func(self)
        if isinstance(results, dict):
            results.update({
                child.key: child.execute(func)
                for child in self.relationships.values()
            })
        else:
            assert isinstance(results, (list, tuple))
        return results


class GraphQLObject(object):

    @classmethod
    def graphql_query(self, node: GraphQLFieldNode, fields: Dict = None):
        raise NotImplementedError('override in subclass')


class GraphQLEngine(object):

    def __init__(self, root):
        self._parser = GraphQLParser()
        self._root = root

    def query(self, query: str) -> dict:
        tree = self._parse_query(query)
        results = {}

        for node in tree.values():
            results.update(node.execute(self._eval_field_node))

        return results

    def _parse_query(self, query: str) -> dict:
        ast_query = self._parser.parse(query).definitions[0]
        tree = {}

        for field in ast_query.selections:
            rel = self._root.relationships.get(field.name)
            if rel:
                node = GraphQLFieldNode(rel, field, None)
                tree[node.key] = node
            else:
                # TODO: use custom exception type
                raise Exception('unrecognized field: {}'.format(field.name))

        return tree

    def _eval_field_node(self, node):
        assert issubclass(node.bizobj_class, GraphQLObject)

        bizobj_class = node.bizobj_class
        schema_class = bizobj_class.Schema

        # "load" the field names so that they appear as they should
        # when received by the bizobj instance.
        fields = schema_class.load_keys(node.fields.keys())

        # load the BizObject with the requested fields
        getter_result = bizobj_class.graphql_query(node, fields=fields)

        # return a plain dict or list of dicts from the result object
        return self._format_result(getter_result)

    def _format_result(self, result):
        if is_bizobj(result):
            return result.dump()
        elif isinstance(result, (list, tuple, set)):
            return [self._format_result(obj) for obj in result]
        elif isinstance(result, dict):
            return result
        raise ValueError(str(result))

