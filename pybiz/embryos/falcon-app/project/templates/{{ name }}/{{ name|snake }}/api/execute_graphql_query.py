from {{ name|snake }}.biz.graphql_document import GraphQLDocument

from . import app


@app.post('/graphql')
def execute_graphql_query(query: str = None):
    """
    Execute a GraphQL query.
    """
    return GraphQLDocument.graphql.query(query)
