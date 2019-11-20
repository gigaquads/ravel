from typing import Type, Set

from .util import is_biz_list, is_biz_object
from .resolver import Resolver, ResolverDecorator
from .biz_thing import BizThing


class Relationship(Resolver):

    def __init__(self, target: Callable, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.target_callback = target
        self.target = None
        self.many = None

    def on_bind(self, biz_class):
        biz_class.app.inject(self.target_callback)
        target = self.target_callback()
        if is_biz_list(target):
            self.target = target.biz_class
            self.many = True
        else:
            assert is_biz_object(target)
            self.target = target
            self.many = False

    def execute(
        self,
        instance: 'BizObject',
        relationship: 'Relationship',
        select: Set = None,
        where: 'Predicate' = None,
        offset: int = None,
        limit: int = None,
    ) -> BizThing:
        super().execute(
            instance, relationship,




class relationship(ResolverDecorator):
    def __init__(self, *args, **kwargs):
        super().__init__(Relationship, *args, **kwargs)
