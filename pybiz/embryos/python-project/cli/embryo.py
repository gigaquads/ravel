from appyratus.validation import fields
from embryo import Embryo


class CliEmbryo(Embryo):
    """
    An embryo for Cli
    """

    class context_schema(Embryo.Schema):
        """
        The respective Cli schema
        - `action`, action being performed on the project
        """
        action = fields.Str()
