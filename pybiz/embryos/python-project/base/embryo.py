from appyratus.validation import schema, fields
from embryo import Embryo


class BaseEmbryo(Embryo):
    """
    An embryo for Base
    """

    class context_schema(schema.Schema):
        """
        The respective Base schema
        - `name`, the name of the python project
        - `description`, a description
        - `version`, a version identifying this project
        - `tagline` a powerful tag line
        """
        name = fields.Str()
        description = fields.Str(default='')
        version = fields.Anything()
        tagline = fields.Str()
