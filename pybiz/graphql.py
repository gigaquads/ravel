from __future__ import absolute_import

from abc import ABCMeta, abstractmethod

from graphql.parser import GraphQLParser


class GraphQLNode(object):
    pass


class GraphQLField(GraphQLNode):

    def __init__(self, relationship, ast_field, parent):
        self.relationship = relationship
        self.parent = parent
        self.name = ast_field.name
        self.alias = getattr(ast_field, 'alias', None)
        self.value = getattr(ast_field, 'value', None)
        self.relationships = {}
        self.fields = {}

        # these are kwargs to pass into the get method of
        # the related bizobj class (if there is one)
        self.kwargs = {
            arg.name: arg.value for arg in
            getattr(ast_field, 'arguments', ())
            }

        # initialize nested relationships and fields
        for child_ast_field in ast_field.selections:
            child_name = child_ast_field.name
            rel = self.bizobj_class.relationships.get(child_name)
            if rel is not None:
                child = GraphQLField(rel, child_ast_field, self)
                self.relationships[child_name] = child
            else:
                child = GraphQLField(None, child_ast_field, self)
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
        results.update({
            child.key: child.execute(func)
            for child in self.relationships.values()
            })
        return results


class GraphQLGetter(object, metaclass=ABCMeta):

    @classmethod
    @abstractmethod
    def graphql_get(self, node: GraphQLField, fields: list=None, **kwargs):
        pass


class GraphQLEngine(object):

    def __init__(self, root):
        self._parser = GraphQLParser()
        self._root = root

    def query(self, query: str) -> dict:
        tree = self._parse_query(query)
        results = {}

        for field in tree.values():
            results.update(field.execute(self._evaluate_field))

        return results

    def _parse_query(self, query: str) -> dict:
        ast_query = self._parser.parse(query).definitions[0]
        tree = {}

        for field in ast_query.selections:
            rel = self._root.relationships.get(field.name)
            if rel:
                graphql_field = GraphQLField(rel, field, None)
                tree[graphql_field.key] = graphql_field
            else:
                # TODO: use custom exception type
                raise Exception('unrecognized field: {}'.format(field.name))

        return tree

    def _evaluate_field(self, field):
        assert issubclass(field.bizobj_class, GraphQLGetter)

        # we "load" the field names so that they appear as they should
        # when passed down into the DAL.
        selected = field.bizobj_class.Schema.load_keys(field.fields.keys())

        # load the BizObject with the requested fields
        bizobj = field.bizobj_class.graphql_get(
            field, fields=selected, **field.kwargs)

        # return said fields in a JSON object
        return bizobj.dump()


if __name__ == '__main__':
    import json

    from datetime import datetime
    from pybiz import Schema, fields, BizObject, Relationship

    class TestObject(BizObject, GraphQLGetter):
        created_at = fields.DateTime(default=lambda: datetime.now())

        @classmethod
        def graphql_get(cls, node, fields=None, id=None, **kwargs):
            return cls(**{k: '1' for k in fields})

    class Account(TestObject):
        name = fields.Str()
        account_type = fields.Str(load_from='type', dump_to='type')

    class User(TestObject):
        name = fields.Str()
        email = fields.Str()
        account = Relationship(Account)

    class Document(TestObject):
        user = Relationship(User)
        account = Relationship(Account)


    query = '''
    {
        user(id: 123) {
            name
            email
            account {
                name
                type
            }
        }
    }'''

    engine = GraphQLEngine(Document)
    print(json.dumps(engine.query(query), indent=2, sort_keys=True))
