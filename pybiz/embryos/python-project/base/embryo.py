from appyratus.validation import schema, fields
from embryo import Embryo


class PythonProjectEmbryo(Embryo):
    """
    An embryo for Python Project
    """

    class context_schema(schema.Schema):
        """
        The respective Python Project schema
        """

        name = fields.Str()
        """
        The name of the python project
        """

        description = fields.Str(default='')
        """
        A description
        """

        version = fields.Anything()
        """
        A version identifying this project
        """

        tagline = fields.Str()
        """
        A sassy tag line
        """

        action = fields.Str()
        """
        Action being performed on the project
        """
