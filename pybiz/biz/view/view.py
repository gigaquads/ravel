from typing import Text, Dict, Callable

from appyratus.schema import Schema

from ..biz_attribute import BizAttribute


class View(BizAttribute):
    def __init__(
        self,
        load: Callable,
        transform: Callable = None,
        schema: Schema = None,
        *args, **kwargs
    ):
        super().__init__(*args, **kwargs)
        self._load = load
        self._transform = transform
        self._schema = schema

    @property
    def schema(self) -> Schema:
        return self._schema

    def query(self, caller: 'BizObject', args: Dict = None) -> object:
        args = args or {}
        data = self._load(caller, **args) 
        if self._transform is not None:
            data = self._transform(caller, data, **args)
        return data
