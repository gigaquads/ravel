from typing import Tuple, Text

from pybiz.util.loggers import console
from pybiz.util.misc_functions import get_class_name
from pybiz.util import is_resource, is_batch
from pybiz.query.order_by import OrderBy
from pybiz.query.request import Request


class ResolverProperty(property):
    def __init__(self, resolver: 'Resolver'):
        self.resolver = resolver
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
