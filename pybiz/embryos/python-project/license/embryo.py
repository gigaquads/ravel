from appyratus.time import utc_now
from appyratus.validation import fields
from embryo import Embryo


class LicenseEmbryo(Embryo):
    """
    # License Embryo
    """

    class context_schema(Embryo.Schema):
        """
        # Context Schema
        The respective License schema
        """
        author = fields.Str()
        year = fields.Str(
            allow_none=True, default=lambda: utc_now().strftime('%Y')
        )
