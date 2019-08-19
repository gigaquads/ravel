class EndpointDecorator(object):
    def __init__(self, app: 'Application', *args, **kwargs):
        super().__init__()
        self.app = app
        self.args = args
        self.kwargs = kwargs

    def __call__(self, func) -> 'Endpoint':
        endpoint = self.app.endpoint_class(func, self)
        self.app.register(endpoint)
        self.app.on_decorate(endpoint)
        return endpoint
