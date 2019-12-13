from random import randint

from typing import Type, List, Set, Text, Callable

from .util import is_biz_list, is_biz_object
from .resolver import Resolver, ResolverDecorator
from .biz_thing import BizThing
from .biz_object import DumpStyle
from .biz_list import BizList


class Relationship(Resolver):

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
                self.BizList = type(
                    'RelationshipBizList',
                    (RelationshipBizList, ),
                    {'biz_class': self._target}
                )
            else:
                self._many = False

            self._target = self._target

        self.BizList = type('RelationshipBizList', (RelationshipBizList, ), {})
        self.BizList.pybiz.biz_class = self._target

    @staticmethod
    def on_post_execute(
        instance: 'BizObject',
        relationship: 'Relationship',
        result: object,
        query: 'Query' = None,
    ):
        if relationship.many and (result is not None):
            return relationship.BizList(
                biz_objects=result,
                relationship=relationship,
                owner=instance,
            )
        else:
            return result

    def generate(self, instance, query=None, *args, **kwarg):
        count = 1 if not self.many else None

        if query is not None:
            count = query.params.get('limit')
            if not count:
                count = randint(1, 10)

        if self.many:
            return self.target.BizList([
                self.target.generate()
                for _ in range(random.randrange(1, 10))
            ])
        else:
            return self.target.generate()

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

    @staticmethod
    def on_add(relationship, index, biz_object):
        pass

    @staticmethod
    def on_rem(relationship, index, biz_object):
        pass


class RelationshipBizList(BizList):
    def __init__(
        self,
        owner: 'BizObject',
        relationship: 'Relationship',
        *args,
        **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.internal.owner = owner
        self.internal.relationship = relationship

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
            self.internal.relationship.on_add(rel, offset + idx, biz_obj)

    def _perform_callback_on_rem(self, offset, biz_objects):
        rel = self.internal.relationship
        for idx, biz_obj in enumerate(biz_objects):
            self.internal.relationship.on_rem(rel, offset + idx, biz_obj)


class relationship(ResolverDecorator):
    def __init__(self, target, *args, **kwargs):
        super().__init__(Relationship, *args, **kwargs)
        self.kwargs['target'] = target
