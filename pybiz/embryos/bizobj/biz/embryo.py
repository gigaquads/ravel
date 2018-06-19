from appyratus.validation import Schema, fields
from embryo.embryo import Embryo


class BizEmbryo(Embryo):
    """
    # Biz Embryo
    """

    class context_schema(Schema):
        """
        # Context Schema
        The respective Biz schema
        
        ## Fields
        * `name`: TODO
        * `fields`: TODO
        """
        name = fields.Str()
        fields = fields.List()
