from pybiz import BizObject, Relationship

from {{ project_name|snake }}.biz.graphql_document import GraphQLDocument

from . import app


@app.get('/graphql')
def execute_graphql_query(q: str=None):
    """
    Execute a GraphQL query string.
    """
    return GraphQLDocument.graphql_engine.query(q)
