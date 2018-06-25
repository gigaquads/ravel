from appyratus.validation import Schema, fields as schema_fields
from embryo.embryo import Embryo


class DaoEmbryo(Embryo):
    """
    # Dao Embryo
    """

    class context_schema(Schema):
        """
        # Context Schema
        The respective Dao schema

        ## Fields
        * `project_name`: TODO
        * `name`: TODO
        * `fields`: TODO
        """
        project_name = schema_fields.Str()
        name = schema_fields.Str()
        dao_type = schema_fields.Str()
        fields = schema_fields.List(nested=schema_fields.Dict())
