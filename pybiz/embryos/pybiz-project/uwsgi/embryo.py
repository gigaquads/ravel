from appyratus.validation import fields
from embryo import Embryo


class UwsgiEmbryo(Embryo):
    """
    # Uwsgi Embryo
    """

    class context_schema(Embryo.Schema):
        """
        # Context Schema
        The respective Uwsgi schema
        """
        pass
