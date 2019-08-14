import pytest
import pybiz

from appyratus.test import mark


class TestBizListBasics(object):
    """
    TODO:
    """

    @mark.unit
    def test_bulk_field_property_getter_works(cls, enterprise_crew):
        expected = [officer.first_name for officer in enterprise_crew]
        assert enterprise_crew.first_name == expected

    @mark.unit
    def test_bulk_field_property_setter_works(cls, enterprise_crew):
        expected = ['Q'] * len(enterprise_crew)
        enterprise_crew.first_name = 'Q'
        assert enterprise_crew.first_name == expected

    @mark.unit
    def test_bulk_field_property_deleter_works(cls, enterprise_crew):
        del enterprise_crew.first_name
        assert set(enterprise_crew.first_name) == {None}

    @mark.integration
    def test_bulk_create_works(cls, enterprise_crew):
        enterprise_crew._id == [None] * len(enterprise_crew)
        enterprise_crew.create()
        assert None not in enterprise_crew._id

    @mark.integration
    def test_bulk_save_works(cls, enterprise_crew):
        assert enterprise_crew[1]._id is None
        enterprise_crew[0]._id = '1' * 32
        enterprise_crew.save()
        assert enterprise_crew[0]._id == '1' * 32
        assert enterprise_crew[1]._id is not None
