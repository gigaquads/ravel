from typing import List, Type, Dict, Set, Text

from appyratus.utils import DictObject

from pybiz.util.misc_functions import get_class_name, is_sequence
from pybiz.constants import IS_BIZ_LIST_ANNOTATION

from .entity import Entity
from .dumper import Dumper, DumpStyle
from .util import is_resource


class BatchMeta(type):
    def __init__(batch_class, name, bases, attr_dict):
        super().__init__(name, bases, attr_dict)
        setattr(batch_class, IS_BIZ_LIST_ANNOTATION, True)
        setattr(batch_class, 'pybiz', DictObject({'biz_class': None}))


class Batch(Entity, metaclass=BatchMeta):
    """
    A multi-Resource. A Batch represents a way of accessing field values and
    other resolvers on a list of Resources.
    """

    def __init__(self, resources: List = None):
        self.internal = DictObject()
        self.internal.data = [
            x if is_resource(x) else self.pybiz.biz_class(data=x)
            for x in resources or []
        ]

    def __getitem__(self, index):
        return self.internal.data[index]

    def __getattr__(self, key):
        """
        Access a resolver by attribute on all Resources contained in this
        Batch. For example, if `users` is a Batch with 2 `User` objects,
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
            f'{get_class_name(self.pybiz.biz_class)}.Batch('
            f'size={len(self)}, dirty={dirty_count})'
        )

    def __add__(self, other):
        """
        Create and return a copy, containing the concatenated data lists.
        """
        clone = type(self)(self.internal.data)

        if isinstance(other, (list, tuple)):
            clone.internal.data.extend(other)
        elif isinstance(other, Batch):
            assert other.pybiz.biz_class is self.pybiz.biz_class
            clone.internal.data.extend(other.internal.data)
        elif is_sequence(other):
            clone.internal.data.extend(other)
        else:
            raise ValueError()

        return clone

    def pprint(self):
        print('[')
        for obj in self.internal.data:
            obj.pprint()
        print(']')

    def append(self, resource: 'Resource'):
        self.internal.data.append(resource)
        return self

    def extend(self, resources: List['Resource']):
        self.internal.data.extend(resources)
        return self

    def insert(self, index: int, resource: 'Resource'):
        self.internal.data.insert(index, resource)
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
            resource._id for resource in self.internal.data
            if resource and resource.is_created
        })
        return self

    def save(self, depth=0):
        self.pybiz.biz_class.save_many(self.internal.data, depth=depth)
        return self

    def clean(self, fields=None):
        for resource in self.internal.data:
            resource.clean(fields=fields)
        return self

    def mark(self, fields=None):
        for resource in self.internal.data:
            resource.mark(fields=fields)
        return self

    def dump(
        self,
        resolvers: Set[Text] = None,
        style: DumpStyle = None,
    ) -> List[Dict]:
        return [
            resource.dump(resolvers=resolvers, style=style)
            for resource in self.internal.data
        ]

    def load(self, resolvers: Set[Text] = None):
        stale_id_2_object = {}
        for resource in self.internal.data:
            if resource and resource._id:
                stale_id_2_object[resource._id] = resource

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

    def unload(self, keys: Set[Text]) -> 'Batch':
        for resource in self.internal.data:
            resource.unload(keys)
        return self
