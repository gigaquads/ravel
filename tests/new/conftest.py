import pytest
import ravel

from ravel import (
    Resource,
    Resolver,
    ResolverManager,
    ResolverProperty,
    ResolverDecorator,
    Relationship,
    Query,
    Request,
    Batch,
    resolver,
    relationship,
)
from ravel.constants import (
    ID_FIELD_NAME,
    REV_FIELD_NAME,
)


@pytest.fixture(scope='function')
def app():
    return ravel.Application().bootstrap()


@pytest.fixture(scope='function')
def BasicResource(app):
    class BasicResource(Resource):
        str_field = ravel.String()
        required_str_field = ravel.String(required=True)
        int_field = ravel.Int()
        nullable_int_field = ravel.Int(nullable=True)
        friend_id = ravel.Id(lambda: BasicResource)

    app.bind(BasicResource)
    return BasicResource


@pytest.fixture(scope='function')
def ResourceWithResolvers(app, BasicResource):
    class ResourceWithResolvers(Resource):

        @resolver(target=BasicResource, nullable=False)
        def basic_friend(self, request):
            return BasicResource(required_str_field='x')

        @relationship(
            join=lambda: (ResourceWithResolvers._id, ResourceWithResolvers._id),
            nullable=False,
        )
        def myself(self, request):
            return request.result

    app.bind(ResourceWithResolvers)
    return ResourceWithResolvers


@pytest.fixture(scope='function')
def basic_query(BasicResource):
    return Query(target=BasicResource)

@pytest.fixture(scope='function')
def basic_resource(BasicResource):
    return BasicResource(
        str_field='x',
        required_str_field='y',
        int_field=1,
        nullable_int_field=None,
        friend_id=None
    ).create()


@pytest.fixture(scope='function')
def BasicBatch(BasicResource):
    return Batch.factory(BasicResource)


@pytest.fixture(scope='function')
def basic_batch(BasicBatch):
    return BasicBatch(indexed=True)
