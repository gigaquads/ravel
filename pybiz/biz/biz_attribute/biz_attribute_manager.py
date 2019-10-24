import bisect

from typing import Type, List, Dict, Tuple, Text
from collections import defaultdict

from pybiz.util.loggers import console

from .biz_attribute import BizAttribute


class BizAttributeManager(object):
    def __init__(self, *args, **kwargs):
        self._name_2_biz_attr = {}
        self._group_map = defaultdict(dict)
        self._ordered_biz_attrs = []

    def __iter__(self):
        return (biz_attr.name for biz_attr in self._ordered_biz_attrs)

    def __getitem__(self, key):
        return self._name_2_biz_attr[key]

    def __contains__(self, key):
        return key in self._name_2_biz_attr

    def __len__(self):
        return len(self._name_2_biz_attr)

    def keys(self):
        return self._name_2_biz_attr.keys()

    def values(self):
        return self._name_2_biz_attr.values()

    def items(self):
        return (
            (biz_attr.name, biz_attr)
            for biz_attr in self._ordered_biz_attrs
        )

    def register(self, name: Text, attr: BizAttribute):
        attr.name = name
        self._name_2_biz_attr[name] = attr
        self._group_map[attr.group][name] = attr
        bisect.insort(self._ordered_biz_attrs, attr)

    def by_name(self, name: Text) -> BizAttribute:
        return self._name_2_biz_attr.get(name, None)

    def by_group(self, group: Text) -> Dict[Text, BizAttribute]:
        return self._group_map[group]

    @property
    def relationships(self):
        return self.by_group(BizAttribute.PybizGroup.relationship)

    @property
    def views(self):
        return self.by_group(BizAttribute.PybizGroup.view)
