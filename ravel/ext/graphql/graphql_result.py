from appyratus.utils import TimeUtils

from ravel.resource import Resource
from ravel.schema import DateTime, String


class GraphQLResult(Resource):
    created_at = DateTime(default=TimeUtils.utc_now, nullable=False, required=True, private=True)
    graphql_query = String(required=True, nullable=False, private=True)
