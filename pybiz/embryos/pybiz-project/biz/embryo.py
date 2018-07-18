from appyratus.validation import fields
from embryo import Embryo, Relationship


class BizEmbryo(Embryo):
    """
    # Biz Embryo

    ## Relationships
    - `project`: TODO
    """

    project = Relationship(name='pybiz-project/base', index=0)

    class context_schema(Embryo.Schema):
        """
        # Context Schema
        The respective Biz schema

        ## Fields
        - `biz`: TODO
          - `fields`: TODO
          - `name`: TODO
          - `type`: TODO
        """
        biz = fields.Object(
            dict(name=fields.Str(), fields=fields.List(nested=fields.Dict()))
        )
        project = fields.Dict()
