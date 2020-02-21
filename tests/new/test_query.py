import pytest
import ravel

from pytest import fixture

from ravel import Resource, Query, Request, OrderBy, resolver
from ravel.constants import ID
from ravel.query.predicate import (
    ConditionalPredicate, BooleanPredicate, Predicate,
    OP_CODE,
)



@fixture(scope='function')
def Node(app):
    class Node(Resource):
        name = ravel.String()
        parent_id = ravel.Id(lambda: Node, nullable=True, default=lambda: None)

        @resolver(target=lambda: Node.Batch)
        def children(self, request):
            query = Query(request=request).where(parent_id=self._id)
            query.select(Node.children)
            return query.execute()

        @resolver(target=lambda: Node)
        def parent(self, request):
            query = Query(request=request).where(_id=self.parent_id)
            return query.execute(first=True)

        @classmethod
        def generate_binary_tree(cls, depth=0) -> 'Node':
            def generate_children(parent, depth):
                if depth > 0:
                    children = cls.Batch.generate(
                        values={'parent_id': parent._id},
                        count=2,
                    ).save()
                    for node in children:
                        generate_children(node, depth - 1)

            depth = max(0, depth)
            root = cls(name='root', parent_id=None).save()
            generate_children(root, depth)
            return root

    app.bind(Node)
    return Node


class TestQueryExecution:
    def test_recursive_execution(self, Node):
        depth = 10
        root = Node.generate_binary_tree(depth=depth)

        query = Node.select(Node.name).where(_id=root._id)

        target = query
        for _ in range(depth - 1):
            request = Node.children.select(Node.name)
            target.select(request)
            target = request

        count = {'value': 0}
        def assert_has_children(parent, depth, count):
            if depth > 0:
                count['value'] += 1
                print(count['value'], depth, parent)
                assert 'children' in parent.internal.state
                assert len(parent.internal.state['children']) == 2
                for child in parent.children:
                    assert_has_children(child, depth - 1, count)

        queried_root = query.execute(first=True)
        assert_has_children(queried_root, depth, count)








def test_query_initializes_correctly(BasicResource, basic_query):
    assert basic_query.target is BasicResource
    assert basic_query.selected is not None
    assert basic_query.parameters is not None
    assert basic_query.options is not None
    assert isinstance(basic_query.selected.fields, dict)
    assert isinstance(basic_query.selected.requests, dict)
    assert not basic_query.selected.fields
    assert not basic_query.selected.requests


def test_select_with_str(basic_query):
    basic_query.select(ID)
    assert ID in basic_query.selected.fields

    req = basic_query.selected.fields[ID]
    assert isinstance(req, Request)


def test_select_with_resolver_property(BasicResource, basic_query):
    basic_query.select(BasicResource._id)
    assert ID in basic_query.selected.fields

    req = basic_query.selected.fields[ID]
    assert isinstance(req, Request)


def test_select_with_request(BasicResource, basic_query):
    req_in = Request(BasicResource._id.resolver)

    basic_query.select(req_in)
    assert ID in basic_query.selected.fields

    req_out = basic_query.selected.fields[ID]
    assert isinstance(req_out, Request)
    assert req_out == req_in


def test_where_predicate_builds(BasicResource, basic_query):
    pred_1 = BasicResource._id == 1
    pred_2 = BasicResource.str_field == 'florp'

    basic_query.where(pred_1)

    assert isinstance(basic_query.parameters.where, ConditionalPredicate)

    basic_query.where(pred_2)

    assert isinstance(basic_query.parameters.where, BooleanPredicate)
    assert basic_query.parameters.where.op == OP_CODE.AND
    assert basic_query.parameters.where.lhs is pred_1
    assert basic_query.parameters.where.rhs is pred_2


@pytest.mark.parametrize('argument, expected', [
    ('_id', OrderBy('_id', desc=False)),
    ('_id asc', OrderBy('_id', desc=False)),
    ('_id desc', OrderBy('_id', desc=True)),
    (OrderBy('_id', desc=True), OrderBy('_id', desc=True)),
    (OrderBy('_id', desc=False), OrderBy('_id', desc=False)),
])
def test_order_by(BasicResource, basic_query, argument, expected):
    basic_query.order_by(argument)
    assert len(basic_query.parameters.order_by) == 1
    assert basic_query.parameters.order_by[0].key == expected.key
    assert basic_query.parameters.order_by[0].desc is expected.desc


def test_returns_basic_result(BasicResource, basic_resource, basic_query):
    query = basic_query.where(BasicResource._id == basic_resource._id)
    result = query.execute(first=True)
    assert result is not None
    assert isinstance(result, BasicResource)
    assert result._id == basic_resource._id
    assert not result.dirty
