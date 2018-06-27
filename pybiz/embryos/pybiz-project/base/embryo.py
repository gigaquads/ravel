from appyratus.validation import fields as schema_fields
from embryo.embryo import Embryo, ContextSchema


class BaseEmbryo(Embryo):
    """
    # Base Embryo
    """

    class context_schema(ContextSchema):
        """
        # Context Schema
        The respective Base schema

        ## Fields
        * `project_name`: TODO
        """
        project_name = schema_fields.Str()

    @property
    def project_name(self):
        return self.context['project_name']
