from typing import Text, Set
from collections import defaultdict
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

    def dump(self, dumper: 'Dumper', value):
        return dumper.dump(value)

    def on_bind(self):
        if self.join_callback is not None:
            self.app.inject(self.join_callback)

        pairs = self.join_callback()
        if not isinstance(pairs[0], (list, tuple)):
            pairs = [pairs]

        self.joins = [Join(l, r) for l, r in pairs]

        self.target = self.joins[-1].right_loader.owner

    def pre_resolve(self, resource, request):
        if request.is_simulated:
            # do nothing because, when simulating, we don't need
            # to waste time trying to fetch data.
            return

        source = resource
        final_join = self.joins[-1]
        results = []

        for j1, j2 in zip(self.joins, self.joins[1:]):
            query = j1.build(source).select(j2.left_field.name)
            result = query.execute()
            results.append(result)
            source = result

        query = final_join.build(source)
        query.select(final_join.right_loader.owner.ravel.resolvers.fields)
        result = query.execute(first=not self.many)
        results.append(result)

        request.result = results[-1]

    def on_backfill(self, resource, request, result):
        raise NotImplementedError()

    def pre_resolve_batch(self, batch, request):
        mappings = []
        source = batch

        for j1, j2 in zip(self.joins, self.joins[1:] + [None]):
            query = j1.build(source)
            if j2 is not None:
                query = query.select(j2.left_field.name)

            queried_resources = defaultdict(set)
            for res in query.execute():
                right_value = res[j1.right_field.name]
                queried_resources[right_value].add(res)

            mapping = {}
            for source_res in source:
                source_value = source_res[j1.left_field.name]
                mapping[source_res] = queried_resources[source_value]

            source = queried_resources
            mappings.append(mapping)

        def extract(resource, mappings, index):
            mapping = mappings[index]
            if index == len(mappings) - 1:
                return mapping[resource]
            else:
                for res in resources:
                    next_resources = mapping[res]
                    return [
                        extract(res, mappings, index + 1)
                        for res in mapping[res]
                    ]

        request.result = []
        for res in batch:
            extracted_resources = extract(res, mappings, 0)
            if self.many:
                request.result.append(self.target.Batch(extracted_resources))
            else:
                request.result.append(
                    list(extracted_resources)[0] if extracted_resources
                    else None
                )

    def on_resolve_batch(self, batch, request):
        return request.result


class Join(object):
    def __init__(self, left, right):
        self.left_loader_property = left
        self.left_loader = left.resolver
        self.left_field = left.resolver.field
        self.right_loader_property = right
        self.right_loader = right.resolver
        self.right_field = right.resolver.field

    def build(self, source) -> 'Query':
        query = self.right_loader_property.resolver.owner.select()

        if is_resource(source):
            source_value = getattr(source, self.left_field.name)
            query.where(self.right_loader_property == source_value)
        else:
            assert is_batch(source)
            left_field_name = self.left_field.name
            source_values = {getattr(res, left_field_name) for res in source}
            query.where(self.right_loader_property.including(source_values))

        return query
