from appyratus.validation import fields
from embryo import Embryo, Relationship


class ApiEmbryo(Embryo):
    """
    # Api Embryo

    ## Relationships
    * `project`: TODO
    """
    project = Relationship(name='pybiz-project/base', index=0)

    class context_schema(Embryo.Schema):
        """
        # Context Schema
        The respective Api schema

        ## Fields
        * `api`:
            * `name`: TODO
        """
        api = fields.Object(dict(name=fields.Str()))
