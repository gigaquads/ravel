import re

from pybiz.util.loggers import console
from pybiz.util.misc_functions import flatten_sequence

from ..query.request import QueryRequest


class ResolverProperty(property):
    """
    All Pybiz-aware attributes at the BizObject class level are instances of
    this class, including field properties, like `User.name`.
    """

    def __init__(self, resolver: 'Resolver'):
        self.resolver = resolver
        self._hash = hash(self.biz_class) + int(
            re.sub(r'[^a-zA-Z0-9]', '', self.resolver.name), 36
        )
        super().__init__(
            fget=self.on_get,
            fset=self.on_set,
            fdel=self.on_del,
        )

    def __hash__(self):
        return self._hash

    @property
    def biz_class(self):
        return self.resolver.biz_class if self.resolver else None

    def select(self, *targets, append=True) -> 'ResolverQuery':
        targets = flatten_sequence(targets)
        return self.resolver.select(*targets)

    def on_get(self, owner: 'BizObject'):
        request = QueryRequest(
            query=self.select(),
            source=owner,
            resolver=self.resolver,
        )
        obj = self.resolver.execute(request)
        if self.resolver.on_get:
            self.resolver.on_get(owner, self.resolver, obj)

        return obj

    def on_set(self, owner: 'BizObject', obj):
        key = self.resolver.name
        old_obj = owner.internal.state.pop(key, None)
        owner.internal.state[key] = obj
        if self.resolver.on_set:
            self.resolver.on_set(self.resolver, old_obj, obj)

    def on_del(self, owner: 'BizObject'):
        key = self.resolver.name
        obj = owner.internal.state.pop(key, None)
        if self.resolver.on_del:
            self.resolver.on_del(self.resolver, obj)
