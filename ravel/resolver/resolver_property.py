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

    @property
    def app(self) -> 'Application':
        return self.resolver.owner.ravel.app

    def select(self, *items: Tuple[Text]):
        request = Request(self.resolver)
        request.select(*items)
        self.resolver.on_select(request)
        return request

    def fget(self, resource: 'Resource'):
        resolver = self.resolver

        # if value not loaded, lazily resolve it
        if resolver.name not in resource.internal.state:
            request = Request(resolver)
            value = resolver.resolve(resource, request)
            if (value is not None) or resolver.nullable:
                resource.internal.state[resolver.name] = value

        value = resource.internal.state.get(resolver.name)
        resolver.on_get(resource, value)
        return value

    def fset(self, resource: 'Resource', new_value):
        resolver = self.resolver
        old_value = resource.internal.state.get(resolver.name)
        resource.internal.state[resolver.name] = new_value
        resolver.on_set(resource, old_value, new_value)

    def fdel(self, resource: 'Resource'):
        resolver = self.resolver
        old_value = resource.internal.state.pop(resolver.name)
        resolver.on_delete(resource, value)
