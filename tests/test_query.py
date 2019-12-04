import os

os.environ['PYBIZ_CONSOLE_LOG_LEVEL'] = 'WARN'

from mock import MagicMock
from appyratus.test import mark

import pytest

import pybiz

from pybiz import Application
from pybiz.constants import ID_FIELD_NAME
from pybiz.biz2.biz_object import BizObject, fields
from pybiz.biz2.relationship import (
    Relationship,
    RelationshipBizList,
    relationship,
)
from pybiz.biz2.resolver import (
    Resolver,
    ResolverProperty,
    resolver,
)
from pybiz.biz2.query import (
    Query,
    QueryParameterAssignment,
    ResolverQuery,
)



@pytest.fixture(scope='function')
def app():
    return Application().bootstrap()


@pytest.fixture(scope='function')
def Thing(app):
    class Thing(BizObject):
        a = fields.String()
        b = fields.Int()

        @relationship(target=lambda: Thing)
        def friend(self, query=None, *args, **kwargs):
            return Thing(a='friend', b=1)

        @relationship(target=lambda: Thing.BizList)
        def owners(self, query=None, *args, **kwargs):
            return [Thing(a='owner', b=2)]


    app.bind(Thing)
    return Thing


@pytest.fixture(scope='function')
def thing_query(Thing):
    return Query(Thing)


@mark.unit
def test_entry_is_made_in_query_params(thing_query):
    assert isinstance(thing_query.foo, QueryParameterAssignment)

    thing_query.where(1)
    thing_query.order_by('a')

    assert 'where' in thing_query.params
    assert thing_query.params['where'] == 1

    assert 'order_by' in thing_query.params
    assert thing_query.params['order_by'] == 'a'


@mark.unit
def test_resolvers_are_selected(Thing, thing_query):
    query = thing_query.select(Thing._id, Thing.a)
    assert {'_id', 'a'} == set(thing_query.params['select'].keys())
    for k, v in thing_query.params['select'].items():
        assert isinstance(v, ResolverQuery)
        assert v.resolver.name == k


@mark.unit
def test_query_generates_correct_dao_call(Thing, thing_query):
    Thing.get_dao = MagicMock()
    thing_query.select(Thing._id, Thing.a)
    thing_query.where(Thing._id != None)
    thing_query.execute()

    assert Thing.get_dao.query.called_once_with(
        predicate=thing_query.params['where'],
        fields={Thing._id, Thing.a}
    )


@mark.unit
def test_relationship_autoconfigures_many(Thing):
    assert Thing.pybiz.resolvers['friend'].many is False
    assert Thing.pybiz.resolvers['owners'].many is True


@mark.unit
def test_generate_recurses_correctly(Thing):
    query = Thing.select(
        Thing.a,
        Thing.b,
        Thing.friend,
    )
    thing = Thing.generate(query)

    #print(thing.internal.state)
    #print(thing.friend.internal.state)

    assert isinstance(thing._id, str)
    assert isinstance(thing.a, str)
    assert isinstance(thing.b, int)
    assert isinstance(thing.friend, Thing)
    assert isinstance(thing.friend.a, str)
    assert isinstance(thing.friend.b, int)
    assert isinstance(thing.friend._id, str)


@mark.unit
def test_generate_recurses_with_query(Thing):
    query = Thing.select(
        Thing.a,
        Thing.b,
        Thing.friend,
    )

    Thing.pybiz.resolvers['friend'].generate = generate_func = MagicMock()

    thing = Thing.generate(query=query)

    Thing.pybiz.resolvers['friend'].generate.assert_called_once_with(
        thing, query=query.params['select']['friend']
    )


@mark.unit
@pytest.mark.parametrize('selectors', [
    tuple(),
    ('_id',),
    ('_id', 'a'),
    ('a'),
    ('b'),
    ('a', 'b', 'friend'),
])
def test_dump_outputs_expected_items(Thing, selectors):
    query = Thing.select(Thing.a, Thing.b, Thing.friend)
    thing = Thing.generate(query)
    data = thing.dump()

    assert data is not None
    assert data.get('_id') is thing._id

    for k in selectors:
        assert k in data
