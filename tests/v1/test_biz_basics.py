import pytest
import pybiz

from appyratus.test import mark

api = pybiz.Api().bootstrap({
    'package': 'pybiz.test.domains.startrek'
})


@pytest.fixture(scope='function')
def captain_picard():
    return api.biz.Person(first_name='Picard', rank='captain')

@pytest.fixture(scope='function')
def leutenant_worf():
    return api.biz.Person(first_name='Worf', rank='leutenant')

@pytest.fixture(scope='function')
def enterprise_crew(captain_picard, leutenant_worf):
    return [
        captain_picard,
        leutenant_worf,
    ]

@pytest.fixture(scope='function')
def the_enterprise():
    return api.biz.Ship(name='Enterprise')


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
    def test_single_object_creates(cls, captain_picard):
        captain_picard.create()

        assert captain_picard._id is not None
        assert captain_picard.first_name == 'Picard'
        assert captain_picard.rank == 'captain'

    @mark.integration
    def test_many_relationship_lazy_loads(cls, captain_picard, the_enterprise):
        the_enterprise.create()
        captain_picard.merge(ship_id=the_enterprise._id).create()

        assert captain_picard.ship is not None
        assert captain_picard.ship._id == the_enterprise._id
        assert isinstance(the_enterprise.crew, api.biz.Person.BizList)
        assert len(the_enterprise.crew) == 1
        assert the_enterprise.crew[0]._id == captain_picard._id

    @mark.integration
    def test_object_is_returned_from_get(cls, captain_picard):
        captain_picard.create()
        gotten = api.biz.Person.get(captain_picard._id)

        assert gotten is not None
        assert gotten._id == captain_picard._id

    @mark.integration
    def test_object_is_returned_with_only_selected_fields(cls, captain_picard):
        captain_picard.create()
        person = api.biz.Person.get(captain_picard._id, select={
            'first_name'
        })
        # ensure only the first_name field is returned (besides _id and _rev)
        assert person.first_name == captain_picard.first_name
        for k in person.internal.record:
            if k not in {'_id', '_rev', 'first_name'}:
                assert person[k] is None

    @mark.integration
    def test_multiple_objects_returned_from_get_many(
        cls, captain_picard, leutenant_worf
    ):
        captain_picard.create()
        leutenant_worf.create()
        people = api.biz.Person.get_many([
            captain_picard._id,
            leutenant_worf._id
        ])

        assert people is not None
        assert set(people._id) == {
            captain_picard._id,
            leutenant_worf._id
        }

    @mark.integration
    def test_ensure_id_and_rev_always_returned_from_get_many(
        cls, captain_picard, leutenant_worf
    ):
        captain_picard.create()
        leutenant_worf.create()
        people = api.biz.Person.get_many([
            captain_picard._id,
            leutenant_worf._id
        ])

        for person in people:
            assert person.internal.record['_id'] is not None
            assert person.internal.record['_rev'] is not None

    @mark.integration
    def test_ensure_id_and_rev_always_returned(cls, captain_picard):
        captain_picard.create()
        person = api.biz.Person.get(captain_picard._id, select={})
        assert person.internal.record['_id'] == captain_picard._id
        assert person.internal.record['_rev'] is not None

    @mark.integration
    def test_ensure_default_field_values_generated_on_create(cls):
        # since the field is not nullable but has a default, it will use the
        # default if al works out.
        recruit = api.biz.Person(first_name='Kompressor', rank=None).create()
        assert recruit.rank == api.biz.Person.schema.fields['rank'].default

    @mark.integration
    def test_ensure_default_field_values_generated_on_create_many(cls):
        # since the field is not nullable but has a default, it will use the
        # default if al works out.
        recruits = api.biz.Person.BizList([
            api.biz.Person(first_name='Kompressor', rank=None),
            api.biz.Person(first_name='Obama', rank=None),
        ]).create()

        recruits.create()
        for person in recruits:
            assert person.rank == api.biz.Person.schema.fields['rank'].default

    @mark.integration
    def test_object_saves_and_fetches_an_update(cls, captain_picard):
        captain_picard.create()
        captain_picard.first_name = 'Locutus of Borg'
        captain_picard.update()

        person = api.biz.Person.get(captain_picard._id)

        assert person._id == captain_picard._id
        assert person.first_name == captain_picard.first_name
