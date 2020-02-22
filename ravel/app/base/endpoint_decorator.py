from typing import Union, Type
from inspect import getmembers, ismethod

from .endpoint import Endpoint


class EndpointDecorator(object):
    def __init__(self, app: 'Application', *args, **kwargs):
        super().__init__()
        self.app = app
        self.args = args
        self.kwargs = kwargs
        self._api_object = None

    def __call__(self, obj) -> Union['Endpoint', Type]:
        if isinstance(obj, type):
            # interpret all non-private methods as endpoint functions
            # and register them all
            api_type = obj
            self._api_object = api_type(self.app)
            predicate = lambda x: (
                (ismethod(x) and x.__name__[0] != '_')
                or isinstance(x, Endpoint)
            )
            for k, v in getmembers(self.api_object, predicate=predicate):
                if isinstance(v, Endpoint):
                    # customize existing endpoint
                    existing_endpoint = self.app.endpoints.get(v.name)
                    if not existing_endpoint:
                        # it's an endpoint but for a different Application
                        # i.e. existing_endpoint.app is not self.app
                        continue

                    kwargs = self.kwargs.copy()
                    kwargs.update(existing_endpoint.decorator.kwargs)

                    new_decorator = type(self)(self.app, *self.args, **kwargs)
                    new_decorator._api_object = self._api_object
                    new_decorator.setup_endpoint(v.target, True)
                else:
                    self.setup_endpoint(v.__func__, False)
            return api_type
        else:
            func = obj
            endpoint = self.setup_endpoint(func, False)
            return endpoint

    def setup_endpoint(self, func, overwrite):
        endpoint = self.app.endpoint_type(func, self)
        self.app.register(endpoint, overwrite=overwrite)
        self.app.on_decorate(endpoint)
        return endpoint

    @property
    def api_object(self) -> 'Api':
        return self._api_object
