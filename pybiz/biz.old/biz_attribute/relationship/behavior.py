from typing import List, Text, Set, Tuple

from appyratus.utils import StringUtils
from pybiz.util.misc_functions import normalize_to_tuple, is_biz_obj


class Behavior(object):
    def __init__(
        self,
        many: bool = None,
        readonly: bool = None,
        private: bool = None,
        lazy: bool = None,
        select: Set = None,
        order_by: Tuple['OrderBy'] = None,
        offset: int = None,
        limit: int = None,
    ):
        # override class callback defs
        self.select = select
        self.many = many
        self.readonly = readonly
        self.private = private
        self.order_by = normalize_to_tuple(order_by)
        self.offset = offset
        self.limit = limit
        self.lazy = lazy
        self.relationship = None

    def on_pre_bootstrap(self, rel: 'Relationship'):
        self.relationship = rel
        if self.on_get is not Behavior.on_get:
            rel.on_get += normalize_to_tuple(self.on_get)
        if self.on_set is not Behavior.on_set:
            rel.on_set += normalize_to_tuple(self.on_set)
        if self.on_add is not Behavior.on_add:
            rel.on_add += normalize_to_tuple(self.on_add)
        if self.on_rem is not Behavior.on_rem:
            rel.on_rem += normalize_to_tuple(self.on_rem)
        if self.on_del is not Behavior.on_del:
            rel.on_del += normalize_to_tuple(self.on_del)
        if self.many is not None:
            rel.many = self.many
        if self.select is not None:
            rel.select = self.select
        if self.order_by is not None:
            rel.order_by = self.order_by
        if self.offset is not None:
            rel.offset = self.offset
        if self.limit is not None:
            rel.limit = self.limit
        if self.lazy is not None:
            rel.lazy = self.lazy
        if self.private is not None:
            rel.private = self.private
        if self.readonly is not None:
            rel.readonly = self.readonly

    def on_post_bootstrap(self, rel: 'Relationship'):
        pass

    def on_get(self, source, target):
        pass

    def on_set(self, source, target):
        pass

    def on_add(self, source, target):
        pass

    def on_rem(self, source, target):
        pass

    def on_del(self, source, target):
        pass
