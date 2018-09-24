from appyratus.validation import schema, fields
from embryo import Embryo


class PythonProjectBaseEmbryo(Embryo):
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
        description = fields.Str(allow_none=True, default='')
        version = fields.Anything(allow_none=True, default='0b0')
        tagline = fields.Str(allow_none=True)
