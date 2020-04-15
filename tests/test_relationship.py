import pytest

from ravel import (
    Resource, Query, String, Id,
    relationship, is_resource, is_batch,
)


@pytest.fixture(scope='function')
def Human(app):
    class Human(Resource):
        name = String(required=True)
        mother_id = Id(lambda: Human, nullable=True)
        father_id = Id(lambda: Human, nullable=True)

        @relationship(join=lambda: (Human.mother_id, Human._id))
        def mother(self, request) -> 'Human':
            return request.result

        @relationship(join=lambda: (Human.father_id, Human._id))
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
def child_count():
    return 2


@pytest.fixture(scope='function')
def mother(Human, child_count):
    mother = Human(name='mother').create()
    for i in range(child_count):
        child = Human(name=f'child_{i+1}', mother_id=mother._id).create()
        custody = Custody(parent_id=mother._id, child_id=child._id).create()
    return mother


class TestRelationship:
    def test_relationship_batch_resolve(self, Human, mother):
        batch = mother.children
        request = Human.children.select()

        Human.ravel.resolvers['mother'].resolve_batch(batch, request)

        assert request.result is not None
        assert isinstance(request.result, dict)
        assert len(request.result) == len(mother.children)
        assert all(c.mother_id == m._id for c, m in request.result.items())

    def test_many_relationship_batch_resolve(self, Human, mother):
        batch = Human.Batch([mother, mother])
        request = Human.children.select()

        Human.ravel.resolvers['children'].resolve_batch(batch, request)

        assert request.result is not None
        assert isinstance(request.result, dict)
        assert len(request.result) == 1  # only one unique element in batch
        assert all(is_batch(x) for x in request.result.values())
        assert all(len(x) == len(mother.children) for x in request.result.values())

    def test_relationship_lazy_loads(self, Human, mother):
        assert mother.mother is None
        assert mother.father is None
        assert mother.children is not None
        assert is_batch(mother.children)
        assert len(mother.children) == 2
        assert all(c.mother_id == mother._id for c in mother.children)
        assert all(c.mother is not None for c in mother.children)
        assert all(isinstance(c.mother, Human) for c in mother.children)

    def test_simulated_relationship_lazy_loading(self, app, Human):
        app.is_simulation = True

        human = Human()

        # lazy simulate father_id
        father_id = human.father_id
        assert father_id is not None

        # lazy simlate father
        assert human.father.is_dirty
        assert human.father._id == human.father_id

    def test_simulated_relationship_query(self, app, Human):
        app.is_simulation = True

        query = Human.select(
            Human.name,
            Human.children,
            Human.father,
        ).where(
            Human.name == 'Sir Lancelot'
        )

        human = query.execute(first=True)

        assert human.is_dirty
        assert human.name == 'Sir Lancelot'
        assert human.father._id == human.father_id
        assert is_batch(human.children)
        assert all(x.is_dirty for x in human.children)
        assert human.children
