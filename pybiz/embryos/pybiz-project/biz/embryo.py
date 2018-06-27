from appyratus.validation import fields as schema_fields
from embryo.embryo import Embryo, ContextSchema


class BizEmbryo(Embryo):
    """
    # Biz Embryo
    """

    class context_schema(ContextSchema):
        """
        # Context Schema
        The respective Biz schema

        ## Fields
        * `biz`: 
            * `name`: TODO
            * `type`: TODO
            * `fields`: TODO
        """
        project_name = schema_fields.Str(allow_none=True)
        biz = schema_fields.Object(
            dict(
                name=schema_fields.Str(),
                fields=schema_fields.List(nested=schema_fields.Dict())
            )
        )

    def on_create(self, project):
        embryos = self.dot.find(name='bizobj/base')
        if embryos:
            self.context['project_name'] = embryos[0].project_name
