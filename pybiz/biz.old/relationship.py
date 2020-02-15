from random import randint
from typing import Type, List, Set, Text, Callable, Union

from appyratus.utils import DictObject

from pybiz.util.misc_functions import is_sequence
from pybiz.predicate import Alias
from pybiz.constants import (
    ID_FIELD_NAME,
)

from .util import is_batch, is_resource
from .resolver.resolver import Resolver
from .resolver.resolver_decorator import ResolverDecorator
from .resolver.resolver_property import ResolverProperty
from .entity import Entity
from .resource import DumpStyle
from .batch import Batch


class Relationship(Resolver):
    def __init__(
        self,
        join: Callable,
        *args,
        **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.join = join
        self.pairs = []
        self.queries = []

    @classmethod
    def tags(cls):
        return {'relationships'}

    @classmethod
    def priority(cls):
        return 10

    def on_bind(self, biz_class):
        # prepare and call "join" callback, returning a sequence of pairs
        # of type Union[ResolverProperty, BatchResolverProperty]
        biz_class.pybiz.app.inject(self.join)
        self.pairs = self.join()
        if isinstance(self.pairs[0], ResolverProperty):
            self.pairs = [self.pairs]

        # the last referenced Resource class in the pairs is the
        # so-called target class.
        self.target_biz_class = self.pairs[-1][1].resolver.biz_class

        # iterativelt build query and subqueries from pairs list
        for idx, (source_prop, target_prop) in enumerate(self.pairs):

            # initialize the next query
            query = target_prop.biz_class.select(target_prop.field.name)

            # add a "where" predicate using an alias to the previous/parent
            # query results or the "owner" Resource of the relationship.
            if not self.queries:
                alias = Alias('$owner')
            else:
                alias = Alias.from_query(self.queries[-1])
                self.queries[-1].select(x=query)

            source_prop_alias = getattr(alias, source_prop.field.name)
            query.where(target_prop.including(source_prop_alias))

            # if this isn't the last query, make sure that we also
            # select the next source field for the following query.
            if idx < (len(self.pairs) - 1):
                next_target_prop = self.pairs[idx + 1][0]
                query.select(next_target_prop.field.name)

            self.queries.append(query)

    @staticmethod
    def on_select(relationship: 'Relationship', query: 'Query'):
        relationship.queries[-1].select(query.params.select)

    @staticmethod
    def pre_execute(
        owner: 'Resource',
        relationship: 'Relationship',
        request: 'QueryRequest'
    ):
        head = relationship.queries[0]
        head.bind({'$owner': owner})

    @staticmethod
    def on_execute(
        owner: 'Resource',
        relationship: 'Relationship',
        request: 'QueryRequest'
    ):
        response = relationship.queries[0].execute(response=True)
        result = response[relationship.queries[-1]]

        if relationship.many:
            return result
        else:
            return result[0] if result else None

    @staticmethod
    def post_execute(
        owner : 'Resource',
        relationship: 'Relationship',
        request: 'QueryRequest',
        result: Union[List, 'Resource']
    ):
        if not isinstance(result, Entity):
            if relationship.many and (not is_batch(result)):
                result = self.target_biz_class.Batch(value)
            elif (not relationship.many) and isinstance(result, dict):
                result = self.target_biz_class(data=result)

        return result

'''

class RelationshipBatch(Batch):
    def __init__(self, resources, owner: 'Resource', *args, **kwargs):
        super().__init__(resources, *args, **kwargs)
        self.internal.owner = owner

    def append(self, resource: 'Resource'):
        super().append(resource)
        self._perform_callback_on_add(max(0, len(self) - 1), [resource])
        return self

    def extend(self, resources: List['Resource']):
        super().extend(resources)
        self._perform_callback_on_add(max(0, len(self) - 1), resources)
        return self

    def insert(self, index: int, resource: 'Resource'):
        super().insert(index, resource)
        self._perform_callback_on_add(index, [resource])
        return self

    def _perform_callback_on_add(self, offset, resources):
        rel = self.internal.relationship
        for idx, resource in enumerate(resources):
            self.pybiz.relationship.on_add(rel, offset + idx, resource)

    def _perform_callback_on_rem(self, offset, resources):
        rel = self.internal.relationship
        for idx, resource in enumerate(resources):
            self.pybiz.relationship.on_rem(rel, offset + idx, resource)

class Relationship(Resolver):

    class Batch(RelationshipBatch):
        pass

    def __init__(
        self,
        join: Callable,
        on_add: Callable = None,
        on_rem: Callable = None,
        *args,
        **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.join = join
        self.joins = []
        self.on_add = on_add or self.on_add
        self.on_rem = on_rem or self.on_rem
        self.Batch = None

    @classmethod
    def tags(cls):
        return {'relationships'}

    @classmethod
    def priority(cls):
        return 10

    @property
    def many(self):
        return self._many

    def on_bind(self, biz_class):
        class Batch(RelationshipBatch):
            pass

        self.Batch = Batch
        self.Batch.pybiz.biz_class = self.target_biz_class
        self.Batch.pybiz.relationship = self

        # now that Resource classes are available through the app,
        # inject them into the lexical scope of the join callback.
        biz_class.pybiz.app.inject(self.join)
        self.joins = self.join()

        assert self.joins

        # if the join callback returned a single pair, normalize
        # it to a list with the single pair as its only element.
        if not isinstance(self.joins[0], (tuple, list)):
            self.joins = [self.joins]

        # set self._target_biz_class, and self._many through the target_biz_class property
        if self.many:
            self.target_biz_class = self.joins[-1][-1].biz_class.Batch
        else:
            self.target_biz_class = self.joins[-1][-1].biz_class

    @staticmethod
    def on_select(relationship: 'Relationship', query: 'Query'):
        import ipdb; ipdb.set_trace()

        # the suffix _prop means that the variable is a property object
        for (source_resolver_prop, target_resolver_prop) in relationship.joins:
            source_resolver = source_resolver_prop.resolver
            source_biz_class = source_resolver.biz_class
            source_field_alias = getattr(getattr(alias, ), source_resolver.name)
            if is_sequence(source_value):
                query.where(target_resolver_prop.including(source_value))
            else:
                query.where(target_resolver_prop == source_value)

        # configure the query to return the first object in the query
        # results to reflect that this is not a "many" relationship.
        query.configure(first=not relationship.many)

    @staticmethod
    def pre_execute(
        owner: 'Resource',
        relationship: 'Relationship',
        request: 'QueryRequest'
    ):
        pass

    @staticmethod
    def on_execute(
        owner: 'Resource',
        relationship: 'Relationship',
        request: 'QueryRequest'
    ):
        import ipdb; ipdb.set_trace()
        return request.query.execute()

    @staticmethod
    def post_execute(
        owner : 'Resource',
        relationship: 'Relationship',
        request: 'QueryRequest',
        result: Union[List, 'Resource']
    ):
        if not isinstance(result, Entity):
            if relationship.many and (not is_batch(result)):
                result = self.Batch(value)
            elif (not relationship.many) and isinstance(result, dict):
                result = self.target_biz_class(data=result)
        return result

    @staticmethod
    def on_backfill(
        owner: 'Resource',
        relationship: 'Relationship',
        request: 'QueryRequest',
        result
    ):
        if self._many:
            batch = result
            limit = request.params.get('limit', 1)
            if len(batch) < limit:
                batch.extend(
                    batch.pybiz.biz_class.generate(request.query)
                    for _ in range(limit - len(batch))
                )
            return batch
        elif result is None:
            return batch.pybiz.biz_class.generate(request.query)

    def generate(self, owner, query):
        return query.generate(first=not self.many)

    def dump(self, dumper: 'Dumper', value):
        """
        NOTE: The built-in Dumper classes do not call Relationship.dump. They
        instead recurse down the Relationship tree using a custom traversal
        algorithm.
        """
        def dump_one(resource):
            return {
                k: resource.pybiz.resolvers[k].dump(dumper, v)
                for k, v in resource.internal.state.items()
            }

        if self._many:
            return [dump_one(resource) for resource in value]
        else:
            return dump_one(resource)

    @staticmethod
    def on_add(
        owner: 'Resource',
        relationship: 'Relationship',
        index: int,
        obj: 'Resource'
    ):
        pass

    @staticmethod
    def on_rem(
        owner: 'Resource',
        relationship: 'Relationship',
        index: int,
        obj: 'Resource'
    ):
        pass
'''
