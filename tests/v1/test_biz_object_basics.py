import pytest
import pybiz

from appyratus.test import mark


class TestBizBasics(object):
    """
    TODO:
    - [] passing data into update, update_many merges and saves
    - [] passing data into create, create_many merges and saves
    - [] delete, delete_many results in None when fetched afterwards
    - [x] "get" methods always return at least _id and _rev
    - [x] "get_many" returns expected
    - [x] defaults are generated on create
    """

    @mark.integration
    def test_single_object_creates(cls, startrek, captain_picard):
        captain_picard.create()

        assert captain_picard._id is not None
        assert captain_picard.first_name == 'Picard'
        assert captain_picard.rank == 'captain'

    @mark.integration
    def test_many_relationship_lazy_loads(cls, startrek, captain_picard, the_enterprise):
        the_enterprise.create()
        captain_picard.merge(ship_id=the_enterprise._id).create()

        assert captain_picard.ship is not None
        assert captain_picard.ship._id == the_enterprise._id
        assert isinstance(the_enterprise.crew, startrek.biz.Officer.BizList)
        assert len(the_enterprise.crew) == 1
        assert the_enterprise.crew[0]._id == captain_picard._id

    @mark.integration
    def test_object_is_returned_from_get(cls, startrek, captain_picard):
        captain_picard.create()
        gotten = startrek.biz.Officer.get(captain_picard._id)

        assert gotten is not None
        assert gotten._id == captain_picard._id

    @mark.integration
    def test_object_is_returned_with_only_selected_fields(
        cls, startrek, captain_picard
    ):
        captain_picard.create()
        officer = startrek.biz.Officer.get(captain_picard._id, select={
            'first_name'
        })
        # ensure only the first_name field is returned (besides _id and _rev)
        assert officer.first_name == captain_picard.first_name
        for k in officer.internal.record:
            if k not in {'_id', '_rev', 'first_name'}:
                assert officer[k] is None

    @mark.integration
    def test_multiple_objects_returned_from_get_many(
        cls, startrek, captain_picard, lieutenant_worf
    ):
        captain_picard.create()
        lieutenant_worf.create()
        people = startrek.biz.Officer.get_many([
            captain_picard._id,
            lieutenant_worf._id
        ])

        assert people is not None
        assert set(people._id) == {
            captain_picard._id,
            lieutenant_worf._id
        }

    @mark.integration
    def test_ensure_id_and_rev_always_returned_from_get_many(
        cls, startrek, captain_picard, lieutenant_worf
    ):
        captain_picard.create()
        lieutenant_worf.create()
        people = startrek.biz.Officer.get_many([
            captain_picard._id,
            lieutenant_worf._id
        ])

        for officer in people:
            assert officer.internal.record['_id'] is not None
            assert officer.internal.record['_rev'] is not None

    @mark.integration
    def test_ensure_id_and_rev_always_returned(cls, startrek, captain_picard):
        captain_picard.create()
        officer = startrek.biz.Officer.get(captain_picard._id, select={})
        assert officer.internal.record['_id'] == captain_picard._id
        assert officer.internal.record['_rev'] is not None

    @mark.integration
    def test_ensure_default_field_values_generated_on_create(cls, startrek):
        # since the field is not nullable but has a default, it will use the
        # default if al works out.
        recruit = startrek.biz.Officer(first_name='Kompressor', rank=None).create()
        assert recruit.rank == startrek.biz.Officer.schema.fields['rank'].default

    @mark.integration
    def test_ensure_default_field_values_generated_on_create_many(cls, startrek):
        # since the field is not nullable but has a default, it will use the
        # default if al works out.
        recruits = startrek.biz.Officer.BizList([
            startrek.biz.Officer(first_name='Kompressor', rank=None),
            startrek.biz.Officer(first_name='Obama', rank=None),
        ]).create()

        recruits.create()

        for officer in recruits:
            rank_field = startrek.biz.Officer.schema.fields['rank']
            assert officer.rank == rank_field.default

    @mark.integration
    def test_object_saves_and_fetches_an_update(cls, startrek, captain_picard):
        captain_picard.create()
        captain_picard.first_name = 'Locutus of Borg'
        captain_picard.update()

        officer = startrek.biz.Officer.get(captain_picard._id)

        assert officer._id == captain_picard._id
        assert officer.first_name == captain_picard.first_name
