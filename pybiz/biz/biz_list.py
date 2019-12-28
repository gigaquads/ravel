from typing import List, Type, Dict, Set, Text

from appyratus.utils import DictObject

from pybiz.util.misc_functions import get_class_name, is_sequence
from pybiz.constants import IS_BIZ_LIST_ANNOTATION

from .biz_thing import BizThing
from .dumper import Dumper, DumpStyle
from .util import is_biz_object


class BizListMeta(type):
    def __init__(biz_list_class, name, bases, attr_dict):
        super().__init__(name, bases, attr_dict)
        setattr(biz_list_class, IS_BIZ_LIST_ANNOTATION, True)
        setattr(biz_list_class, 'pybiz', DictObject({'biz_class': None}))


class BizList(BizThing, metaclass=BizListMeta):
    """
    A multi-BizObject. A BizList represents a way of accessing field values and
    other resolvers on a list of BizObjects.
    """

    def __init__(self, biz_objects: List = None):
        self.internal = DictObject()
        self.internal.data = [
            x if is_biz_object(x) else self.pybiz.biz_class(data=x)
            for x in biz_objects or []
        ]

    def __getitem__(self, index):
        return self.internal.data[index]

    def __getattr__(self, key):
        """
        Access a resolver by attribute on all BizObjects contained in this
        BizList. For example, if `users` is a BizList with 2 `User` objects,
        then doing `users.name` would return `["daniel", "jeff"]`.
        """
        if key not in self.pybiz.biz_class.pybiz.resolvers:
            raise AttributeError(key)
        return [
            getattr(obj, key, None) for obj in self.internal.data
        ]

    def __setattr__(self, key, value):
        if key == 'internal':
            super().__setattr__(key, value)
        else:
            if key in self.pybiz.biz_class.pybiz.resolvers:
                for obj in self.internal.data:
                    setattr(obj, key, value)
            else:
                super().__setattr__(key, value)

    def __len__(self):
        return len(self.internal.data)

    def __bool__(self):
        return bool(self.internal.data)

    def __iter__(self):
        return (x for x in self.internal.data)

    def __repr__(self):
        dirty_count = sum(1 for x in self if x and x.dirty)
        return (
            f'{get_class_name(self.pybiz.biz_class)}.BizList('
            f'size={len(self)}, dirty={dirty_count})'
        )

    def __add__(self, other):
        """
        Create and return a copy, containing the concatenated data lists.
        """
        clone = type(self)(self.internal.data)

        if isinstance(other, (list, tuple)):
            clone.internal.data.extend(other)
        elif isinstance(other, BizList):
            assert other.pybiz.biz_class is self.pybiz.biz_class
            clone.internal.data.extend(other.internal.data)
        elif is_sequence(other):
            clone.internal.data.extend(other)
        else:
            raise ValueError()

        return clone

    def append(self, biz_object: 'BizObject'):
        self.internal.data.append(biz_object)
        return self

    def extend(self, biz_objects: List['BizObject']):
        self.internal.data.extend(biz_objects)
        return self

    def insert(self, index: int, biz_object: 'BizObject'):
        self.internal.data.insert(index, biz_object)
        return self

    def remove(self, target):
        index = self._id.index(target._id)
        del self.internal.data[index]

    def pop(self):
        return self.internal.data.pop()

    def create(self):
        self.pybiz.biz_class.create_many(self.internal.data)
        return self

    def update(self, data: Dict = None, **more_data):
        self.pybiz.biz_class.update_many(self, data=data, **more_data)
        return self

    def delete(self):
        self.pybiz.biz_class.delete_many({
            biz_obj._id for biz_obj in self.internal.data
            if biz_obj and biz_obj.is_created
        })
        return self

    def save(self, depth=0):
        self.pybiz.biz_class.save_many(self.internal.data, depth=depth)
        return self

    def clean(self, fields=None):
        for biz_obj in self.internal.data:
            biz_obj.clean(fields=fields)
        return self

    def mark(self, fields=None):
        for biz_obj in self.internal.data:
            biz_obj.mark(fields=fields)
        return self

    def dump(
        self,
        resolvers: Set[Text] = None,
        style: DumpStyle = None,
    ) -> List[Dict]:
        return [
            biz_obj.dump(resolvers=resolvers, style=style)
            for biz_obj in self.internal.data
        ]

    def load(self, resolvers: Set[Text] = None):
        stale_id_2_object = {}
        for biz_obj in self.internal.data:
            if biz_obj and biz_obj._id:
                stale_id_2_object[biz_obj._id] = biz_obj

        if stale_id_2_object:
            fresh_objects = self.pybiz.biz_class.get_many(
                stale_id_2_object.keys(), select=resolvers
            )
            for fresh_obj in fresh_objects:
                stale_obj = stale_id_2_object.get(fresh_obj._id)
                if stale_obj is not None:
                    stale_obj.merge(fresh_obj)
                    stale_obj.clean(fresh_obj.internal.state.keys())

        return self

    def unload(self, keys: Set[Text]) -> 'BizList':
        for biz_obj in self.internal.data:
            biz_obj.unload(keys)
        return self
