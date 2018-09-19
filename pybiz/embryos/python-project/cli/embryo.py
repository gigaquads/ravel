from appyratus.validation import fields
from embryo import Embryo


class PythonProjectCliEmbryo(Embryo):
    """
    An embryo for Cli
    """

    class context_schema(Embryo.Schema):
        """
        The respective Cli schema
        """
        name = fields.Str()
