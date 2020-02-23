import pytest

from ravel import (
    Resource, Query, String, Id,
    relationship, is_resource, is_batch,
)


@pytest.fixture(scope='function')
def Human(app):
    class Human(Resource):
        name = String(required=True)
        mother_id = Id(lambda: Human)
        father_id = Id(lambda: Human)

        @relationship(join=lambda: (Human.mother_id, Human._id))
        def mother(self, request) -> 'Human':
            return request.result

        @relationship(join=lambda: (Human.mother_id, Human._id))
        def father(self, request) -> 'Human':
            return request.result

        @relationship(
            join=lambda: [
                (Human._id, Custody.parent_id),
                (Custody.child_id, Human._id),
            ],
            many=True
        )
        def children(self, request) -> 'Human.Batch':
            return request.result


    class Custody(Resource):
        parent_id = Id(lambda: Human, required=True)
        child_id = Id(lambda: Human, required=True)


    app.bind([Human, Custody])
    return Human


@pytest.fixture(scope='function')
def mother(Human):
    mother = Human(name='mother').create()
    for i in range(2):
        child = Human(name=f'child_{i}', mother_id=mother._id).create()
        custody = Custody(parent_id=mother._id, child_id=child._id).create()
    return mother


class TestRelationship:
    def test_relationship_pre_resolve(self, Human, mother):
        assert mother.mother is None
        assert mother.father is None
        assert mother.children is not None
        assert is_batch(mother.children)
        assert len(mother.children) == 2
        assert all(c.mother_id == mother._id for c in mother.children)
        assert all(c.mother is not None for c in mother.children)
        assert all(isinstance(c.mother, Human) for c in mother.children)

    def test_relationship_pre_resolve_batch(self, Human, mother):
        batch = mother.children
        request = Human.children.select()

        Human.ravel.resolvers['mother'].resolve_batch(batch, request)

        assert request.result is not None
        assert isinstance(request.result, list)
        assert len(request.result) == len(mother.children)
        assert all(x._id == mother._id for x in request.result)
