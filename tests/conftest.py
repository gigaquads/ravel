import pytest
import pybiz

from pybiz import relationship


@pytest.fixture(scope='function')
def app():
    return pybiz.Application().bootstrap()


@pytest.fixture(scope='function')
def Node(app):

    class Node(pybiz.BizObject):
        name = pybiz.String(required=True)
        parent_id = pybiz.Id(required=True)
        tree_id = pybiz.Id(required=True)

        @relationship(join=lambda: (Node.parent_id, Node._id))
        def parent(self):
            pass

        @relationship(join=lambda: (Node._id, Node.parent_id), many=True)
        def children(self):
            pass

    app.bind(Node)
    return Node


@pytest.fixture(scope='function')
def Tree(app, Node):

    class Tree(pybiz.BizObject):

        root_node_id = pybiz.Id(required=True)
        name = pybiz.String(required=True)

        @relationship(join=lambda: (Tree.root_node_id, Node._id))
        def root(self):
            pass

    app.bind(Tree)
    return Tree


@pytest.fixture(scope='function')
def tree(Tree):
    return Tree(name='Test Tree').save()


@pytest.fixture(scope='function')
def parent(Node, tree):
    root = Node(name='parent', tree_id=tree._id).save()
    tree.merge(root_node_id=root._id).save()
    return root


@pytest.fixture(scope='function')
def children(Node, tree, parent):
    return Node.BizList(
        Node(name=f'child {c}', parent_id=parent._id, tree_id=tree._id)
        for c in 'ABC'
    ).save()
