import pytest
import pybiz

from pybiz.constants import ID_FIELD_NAME
from pybiz.biz.query.order_by import OrderBy
from pybiz.predicate import (
    ConditionalPredicate, BooleanPredicate, Predicate,
    OP_CODE,
)
from pybiz.biz2 import Resource, Query, Request


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
    basic_query.select(ID_FIELD_NAME)
    assert ID_FIELD_NAME in basic_query.selected.fields

    req = basic_query.selected.fields[ID_FIELD_NAME]
    assert isinstance(req, Request)


def test_select_with_resolver_property(BasicResource, basic_query):
    basic_query.select(BasicResource._id)
    assert ID_FIELD_NAME in basic_query.selected.fields

    req = basic_query.selected.fields[ID_FIELD_NAME]
    assert isinstance(req, Request)


def test_select_with_request(BasicResource, basic_query):
    req_in = Request(BasicResource._id.resolver)

    basic_query.select(req_in)
    assert ID_FIELD_NAME in basic_query.selected.fields

    req_out = basic_query.selected.fields[ID_FIELD_NAME]
    assert isinstance(req_out, Request)
    assert req_out == req_in


def test_where_predicate_builds(BasicResource, basic_query):
    pred_1 = BasicResource._id == 1
    pred_2 = BasicResource.str_field == 'florp'

    basic_query.where(pred_1)

    assert isinstance(basic_query.parameters.where, ConditionalPredicate)

    basic_query.where(pred_2, append=True)

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
