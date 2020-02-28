import pytest
import ravel

from pytest import fixture

from ravel.constants import ID, REV
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


@pytest.fixture(scope='function')
def app():
    return ravel.Application().bootstrap()


@fixture(scope='function')
def Tree(app):
    class Tree(Resource):
        parent_id = ravel.Id(lambda: Tree, nullable=True, default=lambda: None)
        root_id = ravel.Id(lambda: Tree, nullable=True, default=lambda: None)
        name = ravel.String(nullable=False, required=True)

        @resolver(target=lambda: Tree.Batch)
        def children(self, request) -> 'Tree.Batch':
            return Query(request=request).where(
                Tree.parent_id == self._id
            ).execute()

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
                            'root_id': parent.root_id,
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
