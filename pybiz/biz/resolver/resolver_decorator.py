from typing import Text, Type, Union, Callable


class ResolverDecorator(object):
    def __init__(
        self,
        resolver_type: Type['Resolver'],
        *args,
        **kwargs
    ):
        self.resolver_type = resolver_type
        self.args = args
        self.kwargs = kwargs

    def __call__(self, func: Callable) -> 'ResolverDecorator':
        self.kwargs['on_resolve'] = func
        return self

    def build_resolver_property(self,
        owner: Type['Resource'], name: Text
    ) -> 'ResolverProperty':
        return self.resolver_type.build_property(
            name=name, owner=owner, *self.args, **self.kwargs
        )
