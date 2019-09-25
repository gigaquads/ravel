from typing import Type, List, Set, Tuple, Text, Dict

from appyratus.utils import DictObject

from pybiz.constants import IS_BIZ_LIST_ANNOTATION
from pybiz.exceptions import RelationshipError
from pybiz.util.misc_functions import repr_biz_id, is_sequence, is_biz_list

from .biz_thing import BizThing


class BizListClassBuilder(object):
    """
    This builder is used to endow each BizObject class with its
    BizObject.BizList attribute, which is a derived BizList class which knows
    about the BizObject class it is associated with. In this way, we are able to
    build accessor properties through which attributes of the stored BizObjects
    can be get and set in batch.
    """

    def build(self, biz_class):
        """
        Create a BizList subclass, specialized for the given BizObject type.
        """
        derived_name = f'{biz_class.__name__}BizList'
        derived_attrs = {IS_BIZ_LIST_ANNOTATION: True, 'biz_class': biz_class}
        biz_list_subclass = type(derived_name, (BizList, ), derived_attrs)

        # create "batch" accessor properties for
        # selectable BizObject attributes
        for name in biz_class.pybiz.all_selectors:
            prop = self._build_property(name)
            setattr(biz_list_subclass, name, prop)

        biz_class.BizList = biz_list_subclass

    def _build_property(self, key):
        """
        Build a property object for a given BizAttribute on the BizObject type
        associated with the BizList subclass.
        """
        def fget(biz_list):
            relationships = biz_list.biz_class.pybiz.attributes.relationships
            rel = relationships.get(key)
            if (rel is not None) and (not rel.many):
                return rel.target_biz_class.BizList([
                    getattr(x, key, None) for x in biz_list
                ])
            else:
                return [
                    getattr(x, key, None) for x in biz_list
                ]

        def fset(biz_list, value):
            for target in biz_list:
                setattr(target, key, value)

        def fdel(biz_list):
            for target in biz_list:
                if hasattr(target, key):
                    delattr(target, key)

        return property(fget=fget, fset=fset, fdel=fdel)


class BizList(BizThing):
    """
    A BizList is a collection of BizObjects with "batch" version of BizObject
    CRUD methods among others base methods. Attributes of the underlying
    colelction of BizObjects can be access through properties on the BizList
    instance, like:

    ```python3
    users = User.BizList([u1, u2])
    assert users._id == [u1._id, u2._id]
    ```
    """

    def __init__(
        self,
        objects: List['BizObject'] = None,
        relationship: 'Relationship' = None,
        source: 'BizObject' = None,
    ):
        """
        If this BizList is the result of being queried through a relationships,
        then both `relationship` and `source` will be defined. The `source` is
        the BizObject which owns the Relationship, and the `relationship` is
        the, well, Relationship through which the BizList was loaded.
        """
        self.internal = DictObject({
            'biz_objects': list(objects or []),
            'relationship': relationship,
            'source': source,
            'arg': None,
        })

    def __getitem__(self, key: int) -> 'BizObject':
        """
        Get one or a slice of a BizList. A slice returns another BizList.
        """
        if isinstance(key, int):
            return self.internal.biz_objects[key]
        elif isinstance(key, slice):
            return self.biz_class.BizList(
                objects=self.internal.biz_objects[key],
                relationship=self.internal.relationship,
                source=self.internal.source
            )
        raise IndexError(key)

    def __len__(self):
        return len(self.internal.biz_objects)

    def __bool__(self):
        return bool(self.internal.biz_objects)

    def __iter__(self):
        return (x for x in self.internal.biz_objects)

    def __repr__(self):
        dirty_count = sum(1 for x in self if x and x.dirty)
        return (
            f'<BizList(type={self.biz_class.__name__}, '
            f'size={len(self)}, dirty={dirty_count})>'
        )

    def __add__(self, other):
        """
        Create and return a copy, containing the concatenated data lists.
        """
        cls = self.__class__
        clone = cls(self.internal.biz_objects, self.internal.relationship, self.internal.source)
        if isinstance(other, (list, tuple)):
            clone._biz_objects += other
        elif isinstance(other, BizList):
            assert self.internal.relationship is other.relationship
            clone._biz_objects += other._biz_objects
        else:
            raise ValueError(str(other))
        return clone

    @classmethod
    def generate(
        cls,
        count: int,
        fields: Set[Text] = None,
        constraints: Dict[Text, 'Constraint'] = None
    ) -> 'BizList':
        return cls(
            cls.biz_class.generate(fields=fields, constraints=constraints)
            for _ in range(max(1, count))
        )

    def create(self):
        self.biz_class.create_many(self.internal.biz_objects)
        return self

    def update(self, data: Dict = None, **more_data):
        self.biz_class.update_many(self, data=data, **more_data)
        return self

    def save(self, depth=1):
        if not depth:
            return self

        to_create = []
        to_update = []

        for biz_obj in self.internal.biz_objects:
            if biz_obj and biz_obj._id is None or '_id' in biz_obj.dirty:
                to_create.append(biz_obj)
            else:
                to_update.append(biz_obj)

        self.biz_class.create_many(to_create)
        self.biz_class.update_many(to_update)

        # recursively save each biz_obj's relationships
        # TODO: optimize this to batch saves and updates here
        for rel in self.biz_class.pybiz.attributes.relationships.values():
            for biz_obj in self.internal.biz_objects:
                biz_thing = biz_obj.internal.attributes.get(rel.name)
                if biz_thing:
                    biz_thing.save(depth=depth-1)

        return self

    def merge(self, obj=None, **more_data):
        if is_sequence(obj) or is_biz_list(obj):
            assert len(obj) == len(self.internal.biz_objects)
            obj_arr = obj
            for biz_obj, obj_to_merge in zip(self.internal.biz_objects, obj_arr):
                biz_obj.merge(obj_to_merge)
            if more_data:
                for biz_obj in self.internal.biz_objects:
                    biz_obj.merge(more_data)
        else:
            for biz_obj in self.internal.biz_objects:
                biz_obj.merge(obj, **more_data)
        return self

    def mark(self, keys=None):
        for biz_obj in self.internal.biz_objects:
            biz_obj.mark(keys=keys)
        return self

    def clean(self, keys=None):
        for biz_obj in self.internal.biz_objects:
            biz_obj.clean(keys=keys)
        return self

    def delete(self):
        self.biz_class.delete_many({
            target._id for target in self.internal.biz_objects
            if (target and target._id)
        })
        return self

    def load(self, selectors: Set[Text] = None):
        if not selectors:
            selectors = set(self.biz_class.Schema.fields.keys())
        elif isinstance(selectors, str):
            selectors = {selectors}

        _ids = [obj._id for obj in self if obj._id is not None]
        results = self.biz_class.get_many(_ids, selectors)

        for stale, fresh in zip(self, results):
            if stale._id is not None:
                stale.merge(fresh)
                stale.clean(fresh.internal.state.keys())

        return self

    def unload(self, keys: Set[Text]) -> 'BizList':
        for biz_obj in self.internal.biz_objects:
            biz_obj.unload(keys)
        return self

    def dump(self, *args, **kwargs) -> List[Dict]:
        return [
            target.dump(*args, **kwargs)
            for target in self.internal.biz_objects
        ]

    def append(self, target: 'BizObject'):
        self._perform_on_add([target])
        self.internal.biz_objects.append(target)
        return self

    def extend(self, biz_objects: List['BizObject']):
        self._perform_on_add(biz_objects)
        self.internal.biz_objects.extend(biz_objects)
        return self

    def insert(self, index: int, target: 'BizObject'):
        self._perform_on_add([target])
        self.internal.biz_objects.insert(index, target)
        return self

    def remove(self, target):
        if self.internal.relationship and self.internal.relationship.on_rem:
            if self.internal.relationship.readonly:
                raise RelationshipError(
                    f'{self.internal.relationship} is read-only'
                )
            do_callbacks = False
            if target is not None:
                try:
                    del self.internal.biz_objects[self._id.index(target._id)]
                    do_callbacks = True
                except ValueError:
                    pass
            if do_callbacks:
                for cb_func in self.internal.relationship.on_rem:
                    cb_func(self.internal.source, target)

    def pop(self, default=None):
        if self.internal.biz_objects:
            return self.remove(self.internal.biz_objects[-1])
        return default

    def _perform_on_add(self, biz_objects):
        if self.internal.relationship and self.internal.relationship.on_add:
            if self.internal.relationship.readonly:
                raise RelationshipError(
                    f'{self.internal.relationship} is read-only'
                )
            for target in biz_objects:
                for cb_func in self.internal.relationship.on_add:
                    cb_func(self.internal.source, target)
