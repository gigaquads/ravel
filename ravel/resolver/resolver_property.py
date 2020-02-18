from typing import Tuple, Text

from ravel.util.loggers import console
from ravel.util.misc_functions import get_class_name
from ravel.util import is_resource, is_batch
from ravel.query.order_by import OrderBy
from ravel.query.request import Request


class ResolverProperty(property):
    def __init__(
        self, resolver: 'Resolver', decorator: 'ResolverDecorator' = None
    ):
        self.resolver = resolver
        self.decorator = decorator
        super().__init__(
            fget=self.fget,
            fset=self.fset,
            fdel=self.fdel,
        )

    def select(self, *items: Tuple[Text]):
        request = Request(self.resolver)
        request.select(items)
        return request

    def fget(self, resource: 'Resource'):
        resolver = self.resolver

        # execute the resolver lazily
        if resolver.name not in resource.internal.state:
            request = Request(resolver)
            value = resolver.resolve(resource, request)
            resource.internal.state[resolver.name] = value

        return resource.internal.state.get(resolver.name)

    def fset(self, resource: 'Resource', value):
        resolver = self.resolver
        resource.internal.state[resolver.name] = value

    def fdel(self, resource: 'Resource'):
        resolver = self.resolver
        resource.internal.state.pop(resolver.name)
