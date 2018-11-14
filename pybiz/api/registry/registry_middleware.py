import inspect

from typing import Dict, Tuple

from .registry_object import RegistryObject



class RegistryMiddleware(RegistryObject):
    def pre_request(self, args: Tuple, kwargs: Dict):
        pass

    def on_request(self, args: Tuple, kwargs: Dict):
        pass

    def post_request(self, args: Tuple, kwargs: Dict, result):
        pass
