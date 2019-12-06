import os

os.environ['PYBIZ_CONSOLE_LOG_LEVEL'] = 'WARN'

from mock import MagicMock
from appyratus.test import mark
from pprint import pprint as pp

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
        label = fields.String()
        size = fields.Int()

        @relationship(target=lambda: Thing)
        def friend(self, query=None, *args, **kwargs):
            return Thing(label='friend', size=1)

        @relationship(target=lambda: Thing.BizList)
        def owners(self, query=None, *args, **kwargs):
            return [Thing(label='owner', size=2)]


    app.bind(Thing)
    return Thing


@pytest.fixture(scope='function')
def thing_query(Thing):
    return Query(Thing)


@mark.unit
def test_entry_is_made_in_query_params(thing_query):
    assert isinstance(thing_query.foo, QueryParameterAssignment)

    thing_query.where(1)
    thing_query.order_by('label')

    assert 'where' in thing_query.params
    assert thing_query.params['where'] == 1

    assert 'order_by' in thing_query.params
    assert thing_query.params['order_by'] == 'label'


@mark.unit
def test_resolvers_are_selected(Thing, thing_query):
    query = thing_query.select(Thing._id, Thing.label)
    assert {'_id', 'label'} == set(thing_query.params['select'].keys())
    for k, v in thing_query.params['select'].items():
        assert isinstance(v, ResolverQuery)
        assert v.resolver.name == k


@mark.unit
def test_query_generates_correct_dao_call(Thing, thing_query):
    Thing.get_dao = MagicMock()
    thing_query.select(Thing._id, Thing.label)
    thing_query.where(Thing._id != None)
    thing_query.execute()

    assert Thing.get_dao.query.called_once_with(
        predicate=thing_query.params['where'],
        fields={Thing._id, Thing.label}
    )


@mark.unit
def test_relationship_autoconfigures_many(Thing):
    assert Thing.pybiz.resolvers['friend'].many is False
    assert Thing.pybiz.resolvers['owners'].many is True


@mark.unit
def test_generate_recurses_correctly(Thing):
    query = Thing.select(
        Thing.label,
        Thing.size,
        Thing.friend,
    )
    thing = Thing.generate(query)

    #print(thing.internal.state)
    #print(thing.friend.internal.state)

    assert isinstance(thing._id, str)
    assert isinstance(thing.label, str)
    assert isinstance(thing.size, int)
    assert isinstance(thing.friend, Thing)
    assert isinstance(thing.friend.label, str)
    assert isinstance(thing.friend.size, int)
    assert isinstance(thing.friend._id, str)


@mark.unit
def test_generate_recurses_with_query(Thing):
    query = Thing.select(
        Thing.label,
        Thing.size,
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
    ('_id', 'label',),
    ('label',),
    ('size',),
    ('label', 'size', 'friend',),
])
def test_dump_outputs_expected_items(Thing, selectors):
    query = Thing.select(Thing.label, Thing.size, Thing.friend)
    thing = Thing.generate(query)
    data = thing.dump()

    pp(data)

    assert data is not None
    assert data.get('_id') == thing._id

    for k in selectors:
        assert k in data


def test_dump_side_loaded_works_with_defaults(Thing):
    query = Thing.select(Thing.label, Thing.friend)
    thing = Thing.generate(query)

    result = thing.dump(style='side_loaded')

    assert result.keys() == {'target', 'links'}
    assert result['target']['_id'] not in result['links']
    assert len(result['links']) == 1

    pp(result)
