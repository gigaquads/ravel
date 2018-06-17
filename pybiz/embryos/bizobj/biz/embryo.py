from appyratus.validation import schema, fields
from embryo.embryo import Embryo


class BizEmbryo(Embryo):
    """
    An embryo for Biz
    """

    class context_schema(fields.Schema):
        """
        The respective Biz schema
        """
        pass
