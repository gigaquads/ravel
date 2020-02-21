import pytest
import ravel

from pytest import fixture

from ravel import (
    Resource,
    Resolver,
    ResolverManager,
    ResolverProperty,
    ResolverDecorator,
    Relationship,
    Query,
    Request,
    Batch,
    resolver,
    relationship,
)
from ravel.constants import (
    ID,
    REV,
)


@pytest.fixture(scope='function')
def app():
    return ravel.Application().bootstrap()


@fixture(scope='function')
def Tree(app):
    class Tree(Resource):
        parent_id = ravel.Id(lambda: Tree, nullable=True, default=lambda: None)
        root_id = ravel.Id(lambda: Tree, nullable=True, default=lambda: None)
        name = ravel.String()

        @resolver(target=lambda: Tree.Batch)
        def children(self, request) -> 'Tree.Batch':
            query = Query(request=request).where(
                Tree.parent_id == self._id
            )
            return query.execute()

        @resolver(target=lambda: Tree)
        def parent(self, request) -> 'Tree':
            return Query(request=request).where(
                Tree._id == self.parent_id
            ).execute(first=True)

        @resolver(target=lambda: Tree)
        def root(self, request) -> 'Tree':
            return Query(request=request).where(
                Tree._id == self.root_id
            ).execute(first=True)

        @classmethod
        def binary_tree_factory(cls, depth=0) -> 'Tree':
            def create_children(parent, depth):
                if depth > 0:
                    children = cls.Batch.generate(
                        values={
                            'parent_id': parent._id,
                            'root_id': parent.root_id or parent._id,
                        },
                        count=2,
                    ).save()
                    for tree in children:
                        create_children(tree, depth - 1)

            root = cls(name='root', parent_id=None)
            root.root_id = root._id
            root.save()

            create_children(root, max(0, depth))

            return root


    app.bind(Tree)
    return Tree



@pytest.fixture(scope='function')
def BasicResource(app):
    class BasicResource(Resource):
        str_field = ravel.String()
        required_str_field = ravel.String(required=True)
        int_field = ravel.Int()
        nullable_int_field = ravel.Int(nullable=True)
        friend_id = ravel.Id(lambda: BasicResource)

    app.bind(BasicResource)
    return BasicResource


@pytest.fixture(scope='function')
def ResourceWithResolvers(app, BasicResource):
    class ResourceWithResolvers(Resource):

        @resolver(target=BasicResource, nullable=False)
        def basic_friend(self, request):
            return BasicResource(required_str_field='x')

        @relationship(
            join=lambda: (ResourceWithResolvers._id, ResourceWithResolvers._id),
        )
        def myself(self, request):
            return request.result

    app.bind(ResourceWithResolvers)
    return ResourceWithResolvers


@pytest.fixture(scope='function')
def basic_query(BasicResource):
    return Query(target=BasicResource)


@pytest.fixture(scope='function')
def basic_resource(BasicResource):
    return BasicResource(
        str_field='x',
        required_str_field='y',
        int_field=1,
        nullable_int_field=None,
    ).create()


@pytest.fixture(scope='function')
def BasicBatch(BasicResource):
    return Batch.factory(BasicResource)


@pytest.fixture(scope='function')
def basic_batch(BasicBatch):
    return BasicBatch(indexed=True)
