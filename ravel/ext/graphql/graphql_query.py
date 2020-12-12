from typing import Text

from appyratus.utils.time_utils import TimeUtils

from ravel.resource import Resource
from ravel.schema import DateTime, String


class GraphqlQuery(Resource):
    created_at = DateTime(default=TimeUtils.utc_now, required=True, private=True)
    source = String()

    @classmethod
    def __abstract__(cls):
        return True

    @classmethod
    def on_bootstrap(cls, *args, **kwargs):
        from .graphql_interpreter import GraphqlInterpreter

        cls._interpreter = GraphqlInterpreter(cls)

    @classmethod
    def interpret(cls, query: Text) -> 'Query':
        """
        Transform the GraphQL query string into a Ravel query.
        """
        return cls._interpreter.interpret(query)
