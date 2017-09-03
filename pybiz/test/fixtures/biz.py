import pytest

from pybiz import schema as fields
from pybiz.biz import BizObject, Relationship
from pybiz.schema import Schema, Int, Str, Object


@pytest.fixture(scope='module')
def BizFields():
    class BizFields(BizObject):
        floaty = fields.Float(allow_none=True)
        floaty_default = fields.Float(default=7.77, allow_none=True)

    return BizFields
