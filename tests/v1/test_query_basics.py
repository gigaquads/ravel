import pytest
import ravel

from appyratus.test import mark


class TestQueryBasics(object):
    """
    TODO:
    """

    @mark.integration
    def test_query_by_id_via_select(cls, startrek, captain_picard):
        Officer = startrek.biz.Officer

        captain_picard.create()
        query = Officer.select().where(Officer._id == captain_picard._id)
        officer = query.execute(first=True)

        # ensure that, when no selectors specified in select(), all field names
        # are queried by default
        assert officer.internal.state.keys() - Officer.schema.fields.keys() == set()

        # ensure that we got the expected object.
        for k, v in captain_picard.internal.state.items():
            assert officer[k] == v

    @mark.integration
    def test_query_by_id_via_get_method(cls, startrek, captain_picard):
        Officer = startrek.biz.Officer

        captain_picard.create()
        officer = Officer.get(captain_picard._id)

        # ensure that, when no selectors specified in select(), all field names
        # are queried by default
        assert officer.internal.state.keys() - Officer.schema.fields.keys() == set()

        # ensure that we got the expected object.
        for k, v in captain_picard.internal.state.items():
            assert officer[k] == v

    @mark.integration
    def test_query_by_id_via_query_method(cls, startrek, captain_picard):
        Officer = startrek.biz.Officer

        captain_picard.create()
        officer = Officer.query(where=(Officer._id == captain_picard._id), first=True)

        # ensure that, when no selectors specified in select(), all field names
        # are queried by default
        assert officer.internal.state.keys() - Officer.schema.fields.keys() == set()

        # ensure that we got the expected object.
        for k, v in captain_picard.internal.state.items():
            assert officer[k] == v

    @mark.integration
    def test_query_by_id_via_select(cls, startrek, captain_picard):
        Officer = startrek.biz.Officer

        captain_picard.create()

        assert captain_picard._id is not None
        assert captain_picard._rev is not None

        officer = Officer.select({'species'}).where(
            Officer._id == captain_picard._id
        ).execute(first=True)

        assert officer.internal.state['species'] == captain_picard.species
        assert officer.internal.state['_id'] == captain_picard._id
        assert officer.internal.state['_rev'] == captain_picard._rev

        for k, v in officer.internal.state.items():
            if k not in {'_id', '_rev', 'species'}:
                assert v is None

    @mark.integration
    def test_query_loads_relationship(cls, startrek, captain_picard, the_enterprise):
        Officer = startrek.biz.Officer

        the_enterprise.create()
        captain_picard.merge(ship_id=the_enterprise._id).create()

        officer = Officer.select({'ship'}).where(
            Officer._id == captain_picard._id
        ).execute(first=True)

        assert officer.ship is not None
        assert officer.ship._id == the_enterprise._id

    @mark.integration
    def test_query_loads_many_relationship(cls, startrek, enterprise_crew, the_enterprise):
        Ship = startrek.biz.Ship

        the_enterprise.create()
        enterprise_crew.merge(ship_id=the_enterprise._id).create()

        ship = Ship.select({'crew'}).where(
            Ship._id == the_enterprise._id
        ).execute(first=True)

        assert set(ship.crew._id) == set(enterprise_crew._id)

    @mark.integration
    def test_query_relationship_respects_order_by(cls, startrek, enterprise_crew, the_enterprise):
        Ship = startrek.biz.Ship

        the_enterprise.create()
        enterprise_crew.merge(ship_id=the_enterprise._id).create()

        # crew members should be ordered by name, ascending
        ship = Ship.select({'crew'}).where(
            Ship._id == the_enterprise._id
        ).execute(first=True)

        for name1, name2 in zip(ship.crew.first_name, ship.crew.first_name[1:]):
            assert name1 <= name2

    @mark.integration
    def test_query_relationship_respects_limit(cls, startrek, enterprise_crew, the_enterprise):
        Ship = startrek.biz.Ship

        the_enterprise.create()
        enterprise_crew.merge(ship_id=the_enterprise._id).create()
        exp_limit = 1

        ship = Ship.select(Ship.crew.select().limit(exp_limit)).where(
            Ship._id == the_enterprise._id
        ).execute(first=True)

        assert len(ship.crew) == exp_limit

    @mark.integration
    def test_query_relationship_respects_offset(cls, startrek, enterprise_crew, the_enterprise):
        Ship = startrek.biz.Ship
        the_enterprise.create()
        enterprise_crew.merge(ship_id=the_enterprise._id).create()
        offset = 1

        # sort crew by name for comparing later to query return value
        sorted_crew = sorted(enterprise_crew, key=lambda x: x.first_name)
        ship = Ship.select(Ship.crew.select().offset(offset)).where(
            Ship._id == the_enterprise._id
        ).execute(first=True)

        assert ship.crew[0].first_name == sorted_crew[offset].first_name

    @mark.integration
    def test_query_empty_relationship_returns_expected_dtypes(
        cls, startrek, captain_picard, the_enterprise
    ):
        captain_picard.create()
        the_enterprise.create()

        assert captain_picard.ship is None
        assert isinstance(the_enterprise.crew, startrek.biz.Officer.Batch)
        assert len(the_enterprise.crew) == 0
