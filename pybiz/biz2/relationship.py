from random import randint
from typing import Type, List, Set, Text, Callable

from appyratus.utils import DictObject

from pybiz.constants import (
    ID_FIELD_NAME,
)

from .util import is_biz_list, is_biz_object
from .resolver import Resolver, ResolverDecorator
from .biz_thing import BizThing
from .biz_object import DumpStyle
from .biz_list import BizList


class RelationshipBizList(BizList):
    def __init__(self, biz_objects, owner: 'BizObject', *args, **kwargs):
        super().__init__(biz_objects, *args, **kwargs)
        self.internal.owner = owner

    def append(self, biz_object: 'BizObject'):
        super().append(biz_object)
        self._perform_callback_on_add(max(0, len(self) - 1), [biz_object])
        return self

    def extend(self, biz_objects: List['BizObject']):
        super().extend(biz_objects)
        self._perform_callback_on_add(max(0, len(self) - 1), biz_objects)
        return self

    def insert(self, index: int, biz_object: 'BizObject'):
        super().insert(index, biz_object)
        self._perform_callback_on_add(index, [biz_object])
        return self

    def _perform_callback_on_add(self, offset, biz_objects):
        rel = self.internal.relationship
        for idx, biz_obj in enumerate(biz_objects):
            self.pybiz.relationship.on_add(rel, offset + idx, biz_obj)

    def _perform_callback_on_rem(self, offset, biz_objects):
        rel = self.internal.relationship
        for idx, biz_obj in enumerate(biz_objects):
            self.pybiz.relationship.on_rem(rel, offset + idx, biz_obj)


class Relationship(Resolver):

    class BizList(RelationshipBizList):
        pass

    def __init__(
        self,
        target: Type['BizObject'],
        on_add: Callable = None,
        on_rem: Callable = None,
        *args,
        **kwargs
    ):
        super().__init__(target=target, *args, **kwargs)

        self.on_add = on_add or self.on_add
        self.on_rem = on_rem or self.on_rem

        self.BizList = None  # <- computed in bind

        # If `many` is set, then we expect this relationship to return a a
        # collection (e.g. a list, BizList) instead of a single BizObject.  this
        # flag is set automatically during the bind lifecycle method of the this
        # Relationship.
        self._many = None

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
        class BizList(RelationshipBizList):
            pass

        self.BizList = BizList
        self.BizList.pybiz.biz_class = self.target
        self.BizList.pybiz.relationship = self

    @staticmethod
    def on_execute(
        owner: 'BizObject',
        relationship: 'Resolver',
        request: 'QueryRequest'
    ):
        return request.query.execute(first=not self.many)

    @staticmethod
    def post_execute(
        owner : 'BizObject',
        relationship: 'Relationship',
        request: 'QueryRequest',
        result
    ):
        if not isinstance(result, BizThing):
            if relationship.many and (not is_biz_list(result)):
                result = self.BizList(value)
            elif (not relationship.many) and isinstance(result, dict):
                result = self.target(data=result)
        return result

    @staticmethod
    def on_backfill(
        owner: 'BizObject',
        relationship: 'Relationship',
        request: 'QueryRequest',
        result
    ):
        if self._many:
            biz_list = result
            limit = request.params.get('limit', 1)
            if len(biz_list) < limit:
                biz_list.extend(
                    biz_list.pybiz.biz_class.generate(request.query)
                    for _ in range(limit - len(biz_list))
                )
            return biz_list
        elif result is None:
            return biz_list.pybiz.biz_class.generate(request.query)

    @staticmethod
    def on_select(
        relationship: 'Relationship',
        query: 'Query',
        parent_query: 'Query'
    ) -> 'ResolverQuery':
        """
        If no fields are selected explicity, then select all by default.
        """
        target = relationship.target
        if query.options.get('eager'):
            required = target.pybiz.resolvers.required_resolvers
            query.select(required)
        return query

    def generate(self, owner, query):
        return query.generate(first=not self.many)

    def dump(self, dumper: 'Dumper', value):
        """
        NOTE: The built-in Dumper classes do not call Relationship.dump. They
        instead recurse down the Relationship tree using a custom traversal
        algorithm.
        """
        def dump_one(biz_obj):
            return {
                k: biz_obj.pybiz.resolvers[k].dump(dumper, v)
                for k, v in biz_obj.internal.state.items()
            }

        if self._many:
            return [dump_one(biz_obj) for biz_obj in value]
        else:
            return dump_one(biz_obj)

    @staticmethod
    def on_add(
        owner: 'BizObject',
        relationship: 'Relationship',
        index: int,
        target: 'BizObject'
    ):
        pass

    @staticmethod
    def on_rem(
        owner: 'BizObject',
        relationship: 'Relationship',
        index: int,
        target: 'BizObject'
    ):
        pass


relationship = Relationship.decorator()
