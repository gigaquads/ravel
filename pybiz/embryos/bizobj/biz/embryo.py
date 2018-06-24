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
        * `project_name`: TODO
        * `name`: TODO
        * `fields`: TODO
        """
        project_name = fields.Str()
        name = fields.Str()
        fields = fields.List(nested=fields.Dict())
