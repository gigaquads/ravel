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

    @mark.integration
    def test_relationship_on_add_callback_for_append(
        cls, startrek, the_enterprise, captain_picard,
    ):
        the_enterprise.save()
        captain_picard.save()

        assert is_bizlist(the_enterprise.crew)
        assert len(the_enterprise.crew) == 0

        the_enterprise.crew.append(captain_picard)

        ship = startrek.biz.Ship.get(the_enterprise._id)

        assert is_bizlist(ship.crew)
        assert len(ship.crew) == 1
        assert ship.crew[0]._id == captain_picard._id

    @mark.integration
    def test_relationship_on_add_callback_for_extend(
        cls, startrek, the_enterprise, captain_picard,
    ):
        the_enterprise.save()
        captain_picard.save()

        assert is_bizlist(the_enterprise.crew)
        assert len(the_enterprise.crew) == 0

        the_enterprise.crew.extend([captain_picard])

        ship = startrek.biz.Ship.get(the_enterprise._id)

        assert is_bizlist(ship.crew)
        assert len(ship.crew) == 1
        assert ship.crew[0]._id == captain_picard._id

    @mark.integration
    def test_relationship_on_add_callback_for_insert(
        cls, startrek, the_enterprise, captain_picard,
    ):
        the_enterprise.save()
        captain_picard.save()

        assert is_bizlist(the_enterprise.crew)
        assert len(the_enterprise.crew) == 0

        the_enterprise.crew.insert(0, captain_picard)

        ship = startrek.biz.Ship.get(the_enterprise._id)

        assert is_bizlist(ship.crew)
        assert len(ship.crew) == 1
        assert ship.crew[0]._id == captain_picard._id

    @mark.integration
    def test_relationship_on_rem_callback_on_pop(
        cls, startrek, the_enterprise, captain_picard,
    ):
        the_enterprise.save()
        captain_picard.save()

        the_enterprise.crew.append(captain_picard)

        ship = startrek.biz.Ship.get(the_enterprise._id)

        assert is_bizlist(ship.crew)
        assert len(ship.crew) == 1
        assert ship.crew[0]._id == captain_picard._id

        ship.crew.pop()
        ship = startrek.biz.Ship.get(the_enterprise._id)

        assert is_bizlist(ship.crew)
        assert len(ship.crew) == 0

    @mark.integration
    def test_relationship_on_rem_callback_on_remove(
        cls, startrek, the_enterprise, captain_picard,
    ):
        the_enterprise.save()
        captain_picard.save()

        the_enterprise.crew.append(captain_picard)

        ship = startrek.biz.Ship.get(the_enterprise._id)

        assert is_bizlist(ship.crew)
        assert len(ship.crew) == 1
        assert ship.crew[0]._id == captain_picard._id

        ship.crew.remove(captain_picard)
        ship = startrek.biz.Ship.get(the_enterprise._id)

        assert is_bizlist(ship.crew)
        assert len(ship.crew) == 0

    @mark.unit
    def test_slicing_produces_another_bizlist(cls, startrek, enterprise_crew):
        enterprise_crew.save()
        sliced = enterprise_crew[:1]
        assert isinstance(sliced, startrek.biz.Officer.BizList)
        assert sliced.source is None
        assert sliced[0]._id == enterprise_crew[0]._id
        assert sliced.relationship is None
        assert not sliced[0].dirty


    @mark.unit
    def test_concat_returns_new_bizlist(cls, enterprise_crew):
        enterprise_crew.save()
        concated = enterprise_crew + enterprise_crew
        assert concated is not enterprise_crew
        assert len(concated) == len(enterprise_crew) * 2
        assert concated[:2]._id == concated[2:]._id
        assert concated.source is None
        assert concated.relationship is None

        for officer in concated:
            assert not officer.dirty
