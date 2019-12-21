from inspect import getmembers, ismethod


class EndpointDecorator(object):
    def __init__(self, app: 'Application', *args, **kwargs):
        super().__init__()
        self.app = app
        self.args = args
        self.kwargs = kwargs

    def __call__(self, obj) -> 'Endpoint':
        endpoints = []

        if isinstance(obj, type):
            endpoint_method_collection_class = obj
            instance = endpoint_method_collection_class()
            for k, v in getmembers(instance, predicate=ismethod):
                endpoints.append(self.setup_endpoint(v))
        else:
            func = obj
            endpoints.append(self.setup_endpoint(func))

        return endpoints

    def setup_endpoint(self, func):
        endpoint = self.app.endpoint_class(func, self)
        self.app.register(endpoint)
        self.app.on_decorate(endpoint)
        return endpoint
