from typing import Type, List, Set, Tuple, Text, Dict

from pybiz.constants import IS_BIZLIST_ANNOTATION
from pybiz.exceptions import RelationshipError
from pybiz.util.misc_functions import repr_biz_id, is_sequence, is_bizlist

from .biz_thing import BizThing


class BizListTypeBuilder(object):
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
        derived_attrs = {IS_BIZLIST_ANNOTATION: True, 'biz_class': biz_class}
        biz_list_subclass = type(derived_name, (BizList, ), derived_attrs)

        # create "batch" accessor properties for
        # selectable BizObject attributes
        for name in biz_class.pybiz.all_selectors:
            prop = self._build_property(name)
            setattr(biz_list_subclass, name, prop)

        return biz_list_subclass

    def _build_property(self, key):
        """
        Build a property object for a given BizAttribute on the BizObject type
        associated with the BizList subclass.
        """
        def fget(biz_list):
            rel = biz_list.biz_class.relationships.get(key)
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
        self._relationship = relationship
        self._targets = list(objects or [])
        self._source = source
        self._arg = None

    def __getitem__(self, key: int) -> 'BizObject':
        """
        Get one or a slice of a BizList. A slice returns another BizList.
        """
        if isinstance(key, int):
            return self._targets[key]
        elif isinstance(key, slice):
            return self.biz_class.BizList(
                self._targets[key], self._relationship, self._source
            )
        raise IndexError(key)

    def __len__(self):
        return len(self._targets)

    def __bool__(self):
        return bool(self._targets)

    def __iter__(self):
        return (x for x in self._targets)

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
        clone = cls(self._targets, self._relationship, self._source)
        if isinstance(other, (list, tuple)):
            clone._targets += other
        elif isinstance(other, BizList):
            assert self._relationship is other.relationship
            clone._targets += other._targets
        else:
            raise ValueError(str(other))
        return clone

    @property
    def relationship(self) -> 'BizObject':
        return self._relationship

    @relationship.setter
    def relationship(self, relationship: 'Relationship'):
        if self._relationship is not None:
            raise ValueError('relationship is readonly')
        self._relationship = relationship

    @property
    def source(self) -> 'BizObject':
        return self._source

    @source.setter
    def source(self, source: 'BizObject'):
        if self._source is not None:
            raise ValueError('source is readonly')
        self._source = source

    @property
    def arg(self):
        return self._arg

    @arg.setter
    def arg(self, value):
        self._arg = value

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
        self.biz_class.create_many(self._targets)
        return self

    def update(self, data: Dict = None, **more_data):
        self.biz_class.update_many(self, data=data, **more_data)
        return self

    def save(self, depth=1):
        if not depth:
            return self

        to_create = []
        to_update = []

        for bizobj in self._targets:
            if bizobj and bizobj._id is None or '_id' in bizobj.dirty:
                to_create.append(bizobj)
            else:
                to_update.append(bizobj)

        self.biz_class.create_many(to_create)
        self.biz_class.update_many(to_update)

        # recursively save each bizobj's relationships
        # TODO: optimize this to batch saves and updates here
        for rel in self.biz_class.relationships.values():
            for bizobj in self._targets:
                biz_thing = bizobj.internal.attributes.get(rel.name)
                if biz_thing:
                    biz_thing.save(depth=depth-1)

        return self

    def merge(self, obj=None, **more_data):
        if is_sequence(obj) or is_bizlist(obj):
            assert len(obj) == len(self._targets)
            obj_arr = obj
            for biz_obj, obj_to_merge in zip(self._targets, obj_arr):
                biz_obj.merge(obj_to_merge)
            if more_data:
                for biz_obj in self._targets:
                    biz_obj.merge(more_data)
        else:
            for biz_obj in self._targets:
                biz_obj.merge(obj, **more_data)
        return self

    def mark(self, keys=None):
        for bizobj in self._targets:
            bizobj.mark(keys=keys)
        return self

    def clean(self, keys=None):
        for bizobj in self._targets:
            bizobj.clean(keys=keys)
        return self

    def delete(self):
        self.biz_class.delete_many({
            target._id for target in self._targets
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
        for bizobj in self._targets:
            bizobj.unload(keys)
        return self

    def dump(self, *args, **kwargs) -> List[Dict]:
        return [
            target.dump(*args, **kwargs)
            for target in self._targets
        ]

    def append(self, target: 'BizObject'):
        self._perform_on_add([target])
        self._targets.append(target)
        return self

    def extend(self, targets: List['BizObject']):
        self._perform_on_add(targets)
        self._targets.extend(targets)
        return self

    def insert(self, index: int, target: 'BizObject'):
        self._perform_on_add([target])
        self._targets.insert(index, target)
        return self

    def remove(self, target):
        if self._relationship and self._relationship.on_rem:
            if self._relationship.readonly:
                raise RelationshipError(
                    f'{self._relationship} is read-only'
                )
            do_callbacks = False
            if target is not None:
                try:
                    del self._targets[self._id.index(target._id)]
                    do_callbacks = True
                except ValueError:
                    pass
            if do_callbacks:
                for cb_func in self._relationship.on_rem:
                    cb_func(self._source, target)

    def pop(self, default=None):
        if self._targets:
            return self.remove(self._targets[-1])
        return default

    def _perform_on_add(self, targets):
        if self._relationship and self._relationship.on_add:
            if self._relationship.readonly:
                raise RelationshipError(f'{self._relationship} is read-only')
            for target in targets:
                for cb_func in self._relationship.on_add:
                    cb_func(self._source, target)
