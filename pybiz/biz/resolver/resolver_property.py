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
        value = None
        if self.resolver.name in owner.internal.state:
            value = owner.internal.state[self.resolver.name]
        elif self.resolver.lazy:
            query = self.select()
            request = QueryRequest(query, source=owner, resolver=self.resolver)
            value = self.resolver.execute(request)
        else:
            # TODO: raise custom exception
            raise Exception(f'{self.resolver.name} not loaded')

        if self.resolver.on_get:
            self.resolver.on_get(owner, self.resolver, value)

        return value

    def on_set(self, owner: 'BizObject', value):
        key = self.resolver.name
        old_value = owner.internal.state.pop(key, None)
        owner.internal.state[key] = value
        if self.resolver.on_set:
            self.resolver.on_set(self.resolver, old_value, value)

    def on_del(self, owner: 'BizObject'):
        key = self.resolver.name
        value = owner.internal.state.pop(key, None)
        if self.resolver.on_del:
            self.resolver.on_del(self.resolver, value)
