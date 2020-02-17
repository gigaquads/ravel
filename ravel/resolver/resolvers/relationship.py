from typing import Text, Set

from ravel.util.loggers import console
from ravel.util.misc_functions import get_class_name, flatten_sequence
from ravel.util import is_resource, is_batch
from ravel.resolver.resolver import Resolver



class Relationship(Resolver):
    def __init__(self, join, *args, **kwargs):
        if callable(join):
            self.join_callback = join
            self.joins = []
        else:
            self.join_callback = None
            self.joins = join

        super().__init__(*args, **kwargs)

    @classmethod
    def tags(cls) -> Set[Text]:
        return {'relationships'}

    @classmethod
    def priority(cls) -> int:
        return 10

    def on_bind(self):
        if self.join_callback is not None:
            self.app.inject(self.join_callback)

        pairs = self.join_callback()
        if not isinstance(pairs[0], (list, tuple)):
            pairs = [pairs]

        self.joins = [Join(l, r) for l, r in pairs]
        self.target = self.joins[-1].right.resolver.owner

    def pre_resolve(self, resource, request):
        if request.is_simulated:
            # do nothing because, when simulating, we don't need
            # to waste time trying to fetch data.
            return

        # TODO: build, execute query, set on request.result
        source = resource
        joins = self.joins
        final_join = joins[-1]

        results = []

        if len(joins) == 1:
            query = final_join.build_query(source)
            query.select(final_join.right.resolver.owner.ravel.resolvers.fields)
            result = query.execute(first=not self.many)
            results.append(result)
        else:
            for j1, j2 in zip(joins, join[1:]):
                query = j1.build_query(source)
                query.select(j2.left.resolver.field.name)
                if j2 is final_join:
                    results.append(query.execute(first=not self.many))
                else:
                    results.append(query.execute())

        request.result = self.target.Batch(results[-1])


class Join(object):
    def __init__(self, left, right):
        self.left = left
        self.right = right

    def build_query(self, source):
        query = self.right.resolver.owner.select()

        if is_resource(source):
            source_value = getattr(source, self.left.resolver.field.name)
            query.where(self.right == source_value)
        else:
            assert is_batch(source)
            source_values = getattr(source, self.left.resolver.field.name)
            query.where(self.right.including(source_values))

        return query
