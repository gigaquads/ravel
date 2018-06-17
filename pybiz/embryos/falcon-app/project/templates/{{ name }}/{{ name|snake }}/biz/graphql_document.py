from pybiz.biz import BizObject, Relationship


class GraphQLDocument(BizObject):
    """
    This class defines the top-level structure permitted by a GraphQL query
    through the use of `Relationship` declarations, for example:

    ```python3
        query = '''
            {
                user {
                    name
                    email
                }
            }
    ```

    This example assumes that a `user` relationship exists below. i.e.
    """

    # user = Relationship(User)  # for example
