from typing import Text, Dict

from ravel.schema import Schema, fields


class Binding:

    class Schema(Schema):
        resource = fields.String()
        store = fields.String()
        params = fields.Dict(default={})


    def __init__(
        self,
        resource_class_name: Text,
        store_class_name: Text,
        bind_params: Dict = None
    ):
        self.resource_class_name = resource_class_name
        self.store_class_name = store_class_name
        self.bind_params = bind_params or {}
        self.resource_class = None
        self.store_class = None