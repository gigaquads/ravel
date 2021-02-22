import inspect

from typing import Text, Set
from collections import defaultdict

from ravel.util.loggers import console
from ravel.util import is_resource, is_batch, get_class_name
from ravel.resolver.resolver import Resolver
from ravel.batch import BatchResolverProperty


class Relationship(Resolver):
    def __init__(self, join, eager=True, order_by=None, *args, **kwargs):
        self._cb_order_by = order_by
        self._order_by = None
        self._eager = eager
        if callable(join):
            self._cb_joins = join
            self._join_sequence = []
        else:
            self._cb_joins = None
            self._join_sequence = join

        super().__init__(*args, **kwargs)

    @classmethod
    def tags(cls) -> Set[Text]:
        return {'relationships'}

    @classmethod
    def priority(cls) -> int:
        return 10

    def dump(self, dumper: 'Dumper', value):
        return dumper.dump(value)

    def on_copy(self, copy):
        copy._cb_order_by = self._cb_order_by
        copy._order_by = None
        copy._eager = self._eager
        copy._cb_joins = self._cb_joins
        copy._join_sequence = self._join_sequence

    def on_bind(self):
        def arg_count(func):
            return len(inspect.signature(func).parameters)

        if self._cb_joins is not None:
            self.app.inject(self._cb_joins)
        if self._cb_order_by is not None:
            self.app.inject(self._cb_order_by)
            if arg_count(self._cb_order_by) > 0:
                self._order_by = self._cb_order_by(self.owner)
            else:
                self._order_by = self._cb_order_by()

        if arg_count(self._cb_joins) > 0:
            pairs = self._cb_joins(self.owner)
        else:
            pairs = self._cb_joins()

        if not isinstance(pairs[0], (list, tuple)):
            pairs = [pairs]

        self._join_sequence = [Join(self, l, r) for l, r in pairs]

        self.target = self._join_sequence[-1].right_loader.owner
        self.many = self._join_sequence[-1].right_many

    def pre_resolve(self, resource, request):
        if self.app.is_simulation:
            # do nothing because, when simulating, we don't need
            # to waste time trying to fetch data.
            return

        source = resource
        final_join = self._join_sequence[-1]
        results = []

        # build sequence of "join queries" to execute, resolving
        # the target Resource type in the final query.
        for j1, j2 in zip(self._join_sequence, self._join_sequence[1:]):
            query = j1.build(source).select(j2.left_field.name)

            if query is None:
                request.result = None
                return

            result = query.execute()
            results.append(result)
            source = result

        # build final query and merge in query parameters
        # passed in through the request
        query = final_join.build(source)
        if query is None:
            request.result = None
            return

        if self._eager:
            query.select(self.target.ravel.resolvers.fields.keys())

        query.merge(request, in_place=True)

        if self._order_by:
            query.order_by(self._order_by)

        result = query.execute(first=not self.many)
        results.append(result)

        # the resolver's returned result is the result from
        # the final query in the "join query" sequence.
        request.result = results[-1]

    def pre_resolve_batch(self, batch, request):
        if self.app.is_simulation:
            # do nothing because, when simulating, we don't need
            # to waste time trying to fetch data.
            return

        mappings = []
        source = batch

        for j1, j2 in zip(
            self._join_sequence,
            self._join_sequence[1:] + [None]
        ):
            query = j1.build(source)

            if query is None:
                request.result = {}
                return

            if j2 is not None:
                # we come here for each Join object except the last
                # in the sequence.
                query.select(j2.left_field.name)
            else:
                # for the final query, merge in query parameters
                # passed in through the request.
                query.merge(request)
                if self._order_by:
                    query.order_by(self._order_by)

            value_2_queried_resource = defaultdict(set)
            queried_resources = query.execute()

            for res in queried_resources:
                right_value = res[j1.right_field.name]
                value_2_queried_resource[right_value].add(res)

            mapping = {}
            for source_res in source:
                source_value = source_res[j1.left_field.name]
                mapping[source_res] = value_2_queried_resource[source_value]

            source = queried_resources
            mappings.append(mapping)

        def extract(key, mappings, index):
            values = mappings[index]
            if index == len(mappings) - 1:
                return values.get(key) or []
            else:
                results = []
                for res in values[key]:
                    extracted_values = extract(res, mappings, index + 1)
                    results.extend(extracted_values)
                return results

        request.result = {}

        for res in batch:
            extracted_resources = extract(res, mappings, 0)
            if self.many:
                request.result[res] = self.target.Batch(extracted_resources)
            else:
                request.result[res] = (
                    list(extracted_resources)[0] if extracted_resources
                    else None
                )

    def on_resolve_batch(self, batch, request):
        return request.result

    def on_simulate(self, resource, request):
        from ravel import Query

        query = Query(request=request)
        if len(self._join_sequence) == 1:
            join = self._join_sequence[0]
            joined_value = getattr(resource, join.left_field.name)
            query.where(join.right_loader_property == joined_value)
        entity = query.execute(first=not self.many)

        return entity


class Join(object):
    def __init__(self, relationship, left, right):
        self.relationship = relationship
        self.left_loader_property = left
        self.left_loader = left.resolver
        self.left_field = left.resolver.field
        self.right_many = False

        if isinstance(right, BatchResolverProperty):
            # in this case, the right-hand field in the join is specified
            # through Batch, like `User.Batch._id`. This is used to indicate
            # that the final query in the Relationship returns "many". This
            # information is already handled before we get here, so at this
            # point, we just replace the batch resolver property with the
            # non-batch one.
            right = getattr(right.resolver.owner, right.resolver.name)
            self.right_many = True

        self.right_loader_property = right
        self.right_loader = right.resolver
        self.right_field = right.resolver.field

    def __repr__(self):
        lhs = (
            f'{get_class_name(self.left_loader.owner)}.'
            f'{self.left_loader.name}'
        )
        rhs = (
            f'{get_class_name(self.right_loader.owner)}.'
            f'{self.right_loader.name}'
        )
        return (
            f'{get_class_name(self)}(from={lhs}, to={rhs})'
        )

    def build(self, source) -> 'Query':
        query = self.right_loader_property.resolver.owner.select()

        if is_resource(source):
            source_value = getattr(source, self.left_field.name)
            if not source_value:
                console.debug(
                    message=(
                        f'relationship {get_class_name(source)}.'
                        f'{self.relationship.name} aborting execution'
                        f'because {self.left_field.name} is None'
                    ),
                    data={
                        'resource': source._id,
                    }
                )
                # NOTE: ^ if you don't want this, then clear the field from
                # source using source.clean(field_name)
                return None
            query.where(self.right_loader_property == source_value)
        else:
            assert is_batch(source)
            left_field_name = self.left_field.name
            source_values = {getattr(res, left_field_name) for res in source}
            if not source_values:
                console.warning(
                    message=(
                        f'relationship {get_class_name(source)}.'
                        f'{self.relationship.name} aborting query '
                        f'because {self.left_field.name} is empty'
                    ),
                    data={
                        'resources': source._id,
                    }
                )
                # NOTE: ^ if you don't want this, then clear the field from
                # source using source.clean(field_name)
                return None
            query.where(self.right_loader_property.including(source_values))

        return query
