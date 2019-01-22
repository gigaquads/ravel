from .registry_object import RegistryObject


class RegistryDecorator(RegistryObject):
    def __init__(self, registry: 'Registry', *args, **kwargs):
        super().__init__()
        self.registry = registry
        self.args = args
        self.kwargs = kwargs

    def __call__(self, func) -> 'RegistryProxy':
        proxy = self.registry.proxy_type(func, self)
        self.registry.proxies.append(proxy)
        self.registry.on_decorate(proxy)
        return proxy
