class ApiDecorator(object):
    def __init__(self, api: 'Api', *args, **kwargs):
        super().__init__()
        self.api = api
        self.args = args
        self.kwargs = kwargs

    def __call__(self, func) -> 'ApiProxy':
        proxy = self.api.proxy_type(func, self)
        self.api.register(proxy)
        self.api.on_decorate(proxy)
        return proxy
