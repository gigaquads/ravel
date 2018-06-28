from appyratus.validation import fields
from embryo.embryo import Embryo, ContextSchema


class BaseEmbryo(Embryo):
    """
    # Base Embryo
    """

    class context_schema(ContextSchema):
        """
        # Context Schema
        The respective Base schema

        ## Fields
        * `project`: TODO
            * `name`: TODO
        """
        project = fields.Object({
            'name': fields.Str(),
        })
