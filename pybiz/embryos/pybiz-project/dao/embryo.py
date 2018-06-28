from appyratus.validation import fields
from embryo import Embryo, Relationship


class DaoEmbryo(Embryo):
    """
    # Dao Embryo

    ## Relationships
    * `project`: TODO
    """
    project = Relationship(name='pybiz-project/base', index=0)

    class context_schema(Embryo.Schema):
        """
        # Context Schema
        The respective Dao schema

        ## Fields
        * `dao`:
            * `name`: TODO
            * `type`: TODO
            * `fields`: TODO
        """
        dao = fields.Object(
            dict(
                name=fields.Str(),
                type=fields.Str(allow_none=True),
                fields=fields.List(nested=fields.Dict())
            )
        )
