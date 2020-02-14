import pytest
import pybiz

from appyratus.test import mark
from pybiz.biz.biz_attribute.relationship import RelationshipProperty
from pybiz.biz.biz_attribute.view import ViewProperty


class TestBizBasics(object):

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
        assert isinstance(the_enterprise.crew, startrek.biz.Officer.Batch)
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
        for k in officer.internal.state:
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
            assert officer.internal.state['_id'] is not None
            assert officer.internal.state['_rev'] is not None

    @mark.integration
    def test_ensure_id_and_rev_always_returned(cls, startrek, captain_picard):
        captain_picard.create()
        officer = startrek.biz.Officer.get(captain_picard._id, select={})
        assert officer.internal.state['_id'] == captain_picard._id
        assert officer.internal.state['_rev'] is not None

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
        recruits = startrek.biz.Officer.Batch([
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

    @mark.unit
    def test_has_expected_biz_attributes(cls, startrek):
        assert 'missions' in startrek.biz.Ship.attributes
        assert 'crew' in startrek.biz.Ship.attributes
        assert 'mission_names' in startrek.biz.Ship.attributes
        assert 'mission_count' in startrek.biz.Ship.attributes
        assert 'mission_count_with_field' in startrek.biz.Ship.attributes

    @mark.unit
    def test_has_expected_biz_attribute_categories(cls, startrek):
        rel_names = startrek.biz.Ship.relationships.keys()
        view_names = startrek.biz.Ship.views.keys()
        assert {'missions', 'crew'} == rel_names
        assert {
            'mission_names',
            'mission_count',
            'mission_count_with_field'
        } == view_names

    @mark.unit
    def test_has_expected_biz_attribute_properties(cls, startrek):
        assert isinstance(startrek.biz.Ship.crew, RelationshipProperty)
        assert isinstance(startrek.biz.Ship.missions, RelationshipProperty)
        assert isinstance(startrek.biz.Ship.mission_names, ViewProperty)
        assert isinstance(startrek.biz.Ship.mission_count, ViewProperty)
        assert isinstance(startrek.biz.Ship.mission_count_with_field, ViewProperty)

    @mark.integration
    def test_save_properly_recurses_default_depth(cls, startrek, the_enterprise, enterprise_crew):
        assert the_enterprise.dirty
        assert the_enterprise._id is None
        assert all([officer.dirty for officer in enterprise_crew])
        assert enterprise_crew._id == ([None] * len(enterprise_crew))

        the_enterprise.crew = enterprise_crew
        the_enterprise.save(depth=-1)

        assert not the_enterprise.dirty
        assert the_enterprise._id is not None
        assert not all([officer.dirty for officer in enterprise_crew])
        assert enterprise_crew._id != ([None] * len(enterprise_crew))

    @mark.integration
    def test_save_properly_recurses_depth_0(cls, startrek, the_enterprise, enterprise_crew):
        assert the_enterprise.dirty
        assert the_enterprise._id is None
        assert all([officer.dirty for officer in enterprise_crew])
        assert enterprise_crew._id == ([None] * len(enterprise_crew))

        the_enterprise.crew = enterprise_crew
        the_enterprise.save(depth=0)

        assert the_enterprise.dirty
        assert the_enterprise._id is None
        assert all([officer.dirty for officer in enterprise_crew])
        assert enterprise_crew._id == ([None] * len(enterprise_crew))

    @mark.integration
    def test_save_properly_recurses_depth_1(cls, startrek, the_enterprise, enterprise_crew):
        assert the_enterprise.dirty
        assert the_enterprise._id is None
        assert all([officer.dirty for officer in enterprise_crew])
        assert enterprise_crew._id == ([None] * len(enterprise_crew))

        the_enterprise.crew = enterprise_crew
        the_enterprise.save(depth=1)

        assert not the_enterprise.dirty
        assert the_enterprise._id is not None
        assert all([officer.dirty for officer in enterprise_crew])
        assert enterprise_crew._id == ([None] * len(enterprise_crew))

    @mark.integration
    def test_save_properly_recurses_depth_2(cls, startrek, the_enterprise, enterprise_crew):
        assert the_enterprise.dirty
        assert the_enterprise._id is None
        assert all([officer.dirty for officer in enterprise_crew])
        assert enterprise_crew._id == ([None] * len(enterprise_crew))

        the_enterprise.crew = enterprise_crew
        the_enterprise.save(depth=2)

        assert not the_enterprise.dirty
        assert the_enterprise._id is not None
        assert not all([officer.dirty for officer in enterprise_crew])
        assert enterprise_crew._id != ([None] * len(enterprise_crew))
