from typing import List, Type, Dict

from appyratus.utils import DictObject

from pybiz.util.misc_functions import get_class_name
from pybiz.constants import IS_BIZ_LIST_ANNOTATION

from .biz_thing import BizThing


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
        self.internal.biz_objects = list(biz_objects or [])

    def __getattr__(self, key):
        """
        Access a resolver by attribute on all BizObjects contained in this
        BizList. For example, if `users` is a BizList with 2 `User` objects,
        then doing `users.name` would return `["daniel", "jeff"]`.
        """
        if key not in self.pybiz.biz_class.pybiz.resolvers:
            raise AttributeError(key)
        return [
            getattr(obj, key, None) for obj in self.internal.biz_objects
        ]

    def __setattr__(self, key, value):
        if key == 'internal':
            super().__setattr__(key, value)
        else:
            if key in self.pybiz.biz_class.pybiz.resolvers:
                for obj in self.internal.biz_objects:
                    setattr(obj, key, value)
            else:
                super().__setattr__(key, value)

    def __len__(self):
        return len(self.internal.biz_objects)

    def __bool__(self):
        return bool(self.internal.biz_objects)

    def __iter__(self):
        return (x for x in self.internal.biz_objects)

    def __repr__(self):
        dirty_count = sum(1 for x in self if x and x.dirty)
        return (
            f'<BizList(type={get_class_name(self.pybiz.biz_class)}, '
            f'size={len(self)}, dirty={dirty_count})>'
        )

    def __add__(self, other):
        """
        Create and return a copy, containing the concatenated data lists.
        """
        clone = type(self)(
            self.pybiz.biz_class,
            self.internal.biz_objects
        )

        if isinstance(other, (list, tuple)):
            clone.internal.biz_objects.extend(other)
        elif isinstance(other, BizList):
            assert other.pybiz.biz_class is self.pybiz.biz_class
            clone.internal.biz_objects.extend(other.internal.biz_objects)
        else:
            raise ValueError()

        return clone

    def append(self, biz_object: 'BizObject'):
        self.internal.biz_objects.append(biz_object)
        return self

    def extend(self, biz_objects: List['BizObject']):
        self.internal.biz_objects.extend(biz_objects)
        return self

    def insert(self, index: int, biz_object: 'BizObject'):
        self.internal.biz_objects.insert(index, biz_object)
        return self

    def remove(self, target):
        index = self._id.index(target._id)
        del self.internal.biz_objects[index]

    def pop(self):
        return self.internal.biz_objects.pop()

    def create(self):
        self.pybiz.biz_class.create_many(self.internal.biz_objects)
        return self

    def update(self, data: Dict = None, **more_data):
        self.pybiz.biz_class.update_many(self, data=data, **more_data)
        return self
