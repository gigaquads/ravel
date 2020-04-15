import pytest
import ravel

from pytest import fixture

from ravel import Resource, Query, Request, OrderBy, resolver
from ravel.constants import ID
from ravel.query.predicate import (
    ConditionalPredicate, BooleanPredicate, Predicate,
    OP_CODE,
)


class TestQueryExecution:
    def test_recursive_execution(self, Tree):
        depth = 10
        root = Tree.binary_tree_factory(depth=depth)
        query = Tree.select(Tree.name).where(_id=root._id)

        # iteratively construct a query for children, children
        # of children, etc. until we've reached the given depth.
        params_owner = query  # type: Union[Query, Request].
        for _ in range(depth - 1):
            request = Tree.children.select(Tree.name)
            params_owner.select(request).limit(2)
            params_owner = request

        count = {'value': 0}

        # function to assert that every node at depth N-1 has children
        # when queried; i.e. that the children property does NOT have
        # to lazy load the value into the state dict but actually comes
        # back from executing the query
        def assert_has_children(parent, depth, count):
            count['value'] += 1
            if depth > 1:
                assert 'children' in parent.internal.state
                assert len(parent.internal.state['children']) == 2
                for child in parent.children:
                    assert_has_children(child, depth - 1, count)

        queried_root = query.execute(first=True)

        assert_has_children(queried_root, depth, count)

        # make sure we have seen all nodes in the recursive procedure
        # above, minus the root node itself, hence the "- 1".
        assert count['value'] == (pow(2, depth) - 1)


class TestQueryDataStructures:
    def test_query_initializes_correctly(self, Tree):
        query = Query(target=Tree)
        assert query.target is Tree
        assert query.requests is not None
        assert query.parameters is not None
        assert query.options is not None
        assert isinstance(query.requests, dict)

    @pytest.mark.parametrize('arg', ['name', ['name'], {'arg': 'name'}])
    def test_select_with_non_ravel_argument(self, Tree, arg):
        query = Query(target=Tree).select(arg)
        assert 'name' in query.requests

        request = query.requests['name']
        assert isinstance(request, Request)

    def test_select_with_resolver_property_argument(self, Tree):
        query = Query(target=Tree).select('name')
        assert 'name' in query.requests

        request = query.requests['name']
        assert isinstance(request, Request)

    def test_select_with_request_argument(self, Tree):
        name_request = Request(Tree.name.resolver)
        query = Query(target=Tree).select(name_request)
        assert 'name' in query.requests
        assert query.requests['name'] is name_request

    def test_select_with_resolver_property_argument(self, Tree):
        name_request = Request(Tree.name.resolver)
        query = Query(target=Tree).select(name_request)
        assert 'name' in query.requests
        assert query.requests['name'] is name_request

    def test_where_predicate_builds(self, Tree):
        pred_1 = Tree._id == 1
        pred_2 = Tree.name == 'florp'
        query = Query(target=Tree).select(ID).where(pred_1)
        assert isinstance(query.parameters.where, ConditionalPredicate)

        query = query.where(pred_2)
        assert isinstance(query.parameters.where, BooleanPredicate)
        assert query.parameters.where.op == OP_CODE.AND
        assert query.parameters.where.lhs is pred_1
        assert query.parameters.where.rhs is pred_2

    @pytest.mark.parametrize('argument, expected', [
        ('_id', OrderBy('_id', desc=False)),
        ('_id asc', OrderBy('_id', desc=False)),
        ('_id desc', OrderBy('_id', desc=True)),
        (OrderBy('_id', desc=True), OrderBy('_id', desc=True)),
        (OrderBy('_id', desc=False), OrderBy('_id', desc=False)),
    ])
    def test_order_by(self, Tree, argument, expected):
        query = Query(target=Tree).select(ID).order_by(argument)
        assert len(query.parameters.order_by) == 1
        assert query.parameters.order_by[0].key == expected.key
        assert query.parameters.order_by[0].desc is expected.desc

    def test_returns_result_no_resolvers(self, Tree):
        tree = Tree(name='root').create()
        query = Query(target=Tree).select(Tree.name).where(_id=tree._id)
        result = query.execute(first=True)
        assert result is not None
        assert isinstance(result, Tree)
        assert result._id == tree._id
        assert not result.is_dirty
