import pytest
import pybiz

from appyratus.test import mark
from pybiz.util.misc_functions import is_bizlist


class TestRelationshipBasics(object):
    """
    TODO:
    """

    @mark.integration
    def test_many_relationship_lazy_loads(cls, startrek, the_enterprise_with_crew):
        the_enterprise_with_crew.save()
        the_enterprise_with_crew.crew.save()

        ship = startrek.biz.Ship.get(the_enterprise_with_crew._id)

        assert 'crew' not in ship.internal.memoized

        ship.crew  # cause lazy loading

        # ensure we get the expected related data back
        assert is_bizlist(ship.internal.memoized['crew'])
        assert len(ship.internal.memoized['crew']) == len(
            the_enterprise_with_crew.crew
        )

        assert ship.crew.source is not None
        assert ship.crew.source._id == the_enterprise_with_crew._id
        assert ship.crew.relationship is the_enterprise_with_crew.relationships['crew']

    @mark.integration
    def test_relationship_lazy_loads(cls, startrek, the_enterprise_with_crew):
        the_enterprise_with_crew.save()
        the_enterprise_with_crew.crew.save()

        officers = startrek.biz.Officer.select().where(
            ship_id=the_enterprise_with_crew._id
        ).execute()

        for officer in officers:
            assert 'ship' not in officer.internal.memoized

        # lazy load all ship relationships via BizList bulk property
        officers.ship

        for officer in officers:
            ship = officer.internal.memoized.get('ship')
            assert ship is not None
            assert ship._id == the_enterprise_with_crew._id
