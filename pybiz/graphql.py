from __future__ import absolute_import

from graphql.parser import GraphQLParser

from pybiz import Schema, fields, BizObject, Relationship


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

        self.kwargs = {
            arg.name: arg.value for arg in
            getattr(ast_field, 'arguments', ())
            }

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


class GraphQLEngine(object):

    def __init__(self, root):
        self._parser = GraphQLParser()
        self._root = root

    def parse(self, query: str) -> dict:
        ast_query = self._parser.parse(query).definitions[0]
        tree = {}

        for field in ast_query.selections:
            rel = self._root.relationships.get(field.name)
            if not rel:
                raise Exception('unrecognized field: {}'.format(field.name))
            field = GraphQLField(rel, field, None)
            tree[field.key] = field

        return tree

    def execute(self, query: str) -> dict:
        tree = self.parse(query)
        results = {}

        for field in tree.values():
            results.update(field.execute(self.evaluate_field_v1_v1))

        return results

    def evaluate_field_v1_v1(self, field):
        bizobj_class = field.bizobj_class
        selected_fields = bizobj_class.Schema.load_keys(field.fields.keys())
        bizobj = bizobj_class.get(fields=selected_fields, **field.kwargs)
        return bizobj.dump()


if __name__ == '__main__':
    import json

    from datetime import datetime

    class TestObject(BizObject):
        created_at = fields.DateTime(default=lambda: datetime.now())

        @classmethod
        def get(cls, fields=None, id=None, **kwargs):
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
    print(json.dumps(engine.execute(query), indent=2, sort_keys=True))
