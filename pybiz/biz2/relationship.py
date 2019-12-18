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
        target: Callable,
        on_add: Callable = None,
        on_rem: Callable = None,
        *args,
        **kwargs
    ):
        super().__init__(*args, **kwargs)

        self.on_add = on_add or self.on_add
        self.on_rem = on_rem or self.on_rem

        self.BizList = None  # <- computed in bind

        # If `many` is set, then we expect this relationship to return a a
        # collection (e.g. a list, BizList) instead of a single BizObject.  this
        # flag is set automatically during the bind lifecycle method of the this
        # Relationship.
        self._many = None

        # if `target` was not provided as a callback but as a class object,
        # we can eagerly set self._target. otherwise, we can only call the
        # callback lazily, during the bind lifecycle method, after its lexical
        # scope has been updated with references to the BizObject types picked
        # up by the host Application.
        if isinstance(target, type):
            self._target_callback = None
            self._target = target
        else:
            self._target_callback = target
            self._target = None

    @classmethod
    def tags(cls):
        return {'relationships'}

    @classmethod
    def priority(cls):
        return 10

    @property
    def target(self):
        return self._target

    @property
    def many(self):
        return self._many

    def on_bind(self, biz_class):
        if self._target_callback:
            biz_class.pybiz.app.inject(self._target_callback)
            obj = self._target_callback()
            if is_biz_list(obj):
                self._target = obj.pybiz.biz_class
                self._many = True
            else:
                self._target = obj
                self._many = False
        else:
            if is_biz_list(self._target):
                self._many = True
            else:
                self._many = False

            self._target = self._target

        class BizList(RelationshipBizList):
            pass

        self.BizList = BizList
        self.BizList.pybiz.biz_class = self._target
        self.BizList.pybiz.relationship = self

    def on_post_execute(self, target, request: 'QueryRequest', result):
        relationship = request.resolver
        transformed_result = result
        if relationship.many and (result is not None):
            biz_objects = result
            transformed_result = self.BizList(biz_objects, owner=target)
        return transformed_result

    def on_backfill(self, instance, request, result):
        if self._many:
            biz_list = result
            limit = request.query.params.get('limit', 1)
            if len(biz_list) < limit:
                biz_list.extend(
                    biz_list.pybiz.biz_class.generate(request.query)
                    for _ in range(limit - len(biz_list))
                )
            return biz_list
        elif result is None:
            return biz_list.pybiz.biz_class.generate(request.query)

    def on_select(self, selectors) -> 'ResolverQuery':
        """
        Ensure that at least _id is selected, and if nothing at all is selected,
        then select all by default.
        """
        from pybiz.biz2.query import ResolverQuery

        query = ResolverQuery(resolver=self, biz_class=self.target)
        biz_class = query.resolver.target

        if not selectors:
            query = query.select(biz_class.pybiz.resolvers.fields.values())
        else:
            query = query.select(*selectors)

        if ID_FIELD_NAME not in query.params['select']:
            query = query.select(biz_class._id)

        return query

    def generate(self, instance, query):
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
    def on_add(relationship, index, biz_object):
        pass

    @staticmethod
    def on_rem(relationship, index, biz_object):
        pass


class relationship(ResolverDecorator):
    def __init__(self, target, *args, **kwargs):
        super().__init__(Relationship, *args, **kwargs)
        self.kwargs['target'] = target
