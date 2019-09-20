import pytest
import pybiz

from appyratus.test import mark
from pybiz.util.misc_functions import is_biz_list


class TestViewBasics(object):
    """
    TODO:
    """

    @mark.integration
    def test_view_loads(cls, startrek, the_enterprise, missions):
        missions.merge(ship_id=the_enterprise.save()._id).save()
        assert set(the_enterprise.mission_names) == set(missions.name)

    @mark.integration
    def test_view_with_transform_loads(cls, startrek, the_enterprise, missions):
        missions.merge(ship_id=the_enterprise.save()._id).save()
        assert the_enterprise.mission_count == len(missions.name)

    @mark.integration
    def test_view_with_transform_loads_applies_field(
        cls, startrek, the_enterprise, missions
    ):
        missions.merge(ship_id=the_enterprise.save()._id).save()
        assert isinstance(the_enterprise.mission_count_with_field, int)
        assert the_enterprise.mission_count_with_field == len(missions.name)
