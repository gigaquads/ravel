from typing import Text, Dict

from ravel.schema import Schema, fields


class Bootstrap:

    class Schema(Schema):
        store = fields.String()
        default = fields.Bool(default=False)
        params = fields.Dict(default={})

    def __init__(
        self,
        store_class_name: Text,
        bootstrap_params: Dict = None,
        is_default: bool = False,
    ):
        self.store_class_name = store_class_name
        self.bootstrap_params = bootstrap_params or {}
        self.is_default = is_default