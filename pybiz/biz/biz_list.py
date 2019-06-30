from functools import reduce

from typing import Type, List, Set, Tuple, Text, Dict
from uuid import UUID

from pybiz.util import repr_biz_id
from pybiz.constants import IS_BIZLIST_ANNOTATION, IS_BIZOBJ_ANNOTATION
from pybiz.exc import RelationshipError

from .view import View, ViewProperty


class FilterableList(list):
    def where(self, *filters):
        filtered = FilterableList()
        for obj in self:
            for keep in filters:
                if not keep(obj):
                    continue
                filtered.append(obj)
        return filtered


class BizList(object):
    @classmethod
    def type_factory(cls, biz_type: Type['BizObject']):
        derived_name = f'{biz_type.__name__}BizList'
        derived_type = type(
            derived_name, (cls, ), {
                IS_BIZLIST_ANNOTATION: True,
                'biz_type': biz_type,
            }
        )

        def build_property(key):
            if key in biz_type.relationships:
                collection_type = biz_type.BizList
            else:
                collection_type = FilterableList

            return property(
                fget=lambda self: collection_type(
                    getattr(bizobj, key, None)
                    for bizobj in self._bizobj_arr
                ),
                fset=lambda self, value: [
                    setattr(bizobj, key, value)
                    for bizobj in self._bizobj_arr
                ]
            )

        for field_name in biz_type.Schema.fields:
            prop = build_property(field_name)
            setattr(derived_type, field_name, prop)

        for rel_name, rel in biz_type.relationships.items():
            prop = build_property(rel_name)
            setattr(derived_type, rel_name, prop)

        return derived_type

    def __init__(
        self,
        objects: List['BizObject'] = None,
        relationship: 'Relationship' = None,
        bizobj: 'BizObject' = None,
    ):
        self.relationship = relationship
        self._bizobj_arr = list(objects or [])
        self.bizobj = bizobj

    def __getattr__(self, attr: Text):
        if attr in self.biz_type.relationships:
            collection_type = biz_type.BizList
        else:
            collection_type = FilterableList
        if attr != IS_BIZLIST_ANNOTATION and attr != IS_BIZOBJ_ANNOTATION:
            return collection_type(
                getattr(bizobj, attr, None) for
                bizobj in self._bizobj_arr
            )
        raise AttributeError(attr)

    def __getitem__(self, idx: int) -> 'BizObject':
        return self._bizobj_arr[idx]

    def __len__(self):
        return len(self._bizobj_arr)

    def __bool__(self):
        return bool(self._bizobj_arr)

    def __iter__(self):
        return iter(self._bizobj_arr)

    def __repr__(self):
        id_parts = []
        for bizobj in self._bizobj_arr:
            id_str = repr_biz_id(bizobj)
            if bizobj is not None:
                dirty_flag = '*' if bizobj.dirty else ''
            else:
                dirty_flag = ''
            id_parts.append(f'{id_str}{dirty_flag}')
        ids = ', '.join(id_parts)
        return (
            f'<BizList(type={self.biz_type.__name__}, '
            f'size={len(self)}, ids=[{ids}])>'
        )

    def __add__(self, other):
        """
        Create and return a copy, containing the concatenated data lists.
        """
        clone = self.copy()
        if isinstance(other, (list, tuple)):
            clone._bizobj_arr += other
        elif isinstance(other, BizList):
            assert self.relationship is other.relationship
            clone._bizobj_arr += other._bizobj_arr
        else:
            raise ValueError(str(other))
        return clone

    def where(self, *filters):
        filtered = []
        for obj in self:
            for keep in filters:
                if not keep(obj):
                    continue
                filtered.append(obj)
        return self.biz_type.BizList(filtered)

    def copy(self):
        cls = self.__class__
        return cls(self._bizobj_arr, self.relationship, self.bizobj)

    def create(self):
        self.biz_type.create_many(self._bizobj_arr)
        return self

    def update(self, data: Dict = None, **more_data):
        self.biz_type.update_many(self, data=data, **more_data)
        return self

    def merge(self, obj=None, **more_data):
        for obj in self._bizobj_arr:
            obj.merge(obj, **more_data)
        return self

    def each(self, func):
        for idx, bizobj in enumerate(self._bizobj_arr):
            func(idx, bizobj)
        return self

    def mark(self, keys=None):
        for bizobj in self._bizobj_arr:
            bizobj.mark(keys=keys)
        return self

    def clean(self, keys=None):
        for bizobj in self._bizobj_arr:
            bizobj.clean(keys=keys)
        return self

    def save(self, *args, **kwargs):
        return self.biz_type.save_many(self._bizobj_arr, *args, **kwargs)

    def delete(self):
        ids = [bizobj for bizobj in self._bizobj_arr if bizobj._id]
        return self.biz_type.delete_many(ids)
        return self

    def load(self, fields: Set[Text] = None):
        # TODO: add a depth=None kwarg like in BizObject.load
        if not fields:
            fields = set(self.biz_type.schema.fields.keys())
        elif isinstance(fields, str):
            fields = {fields}
        _ids = [obj._id for obj in self if obj._id is not None]
        results = self.biz_type.get_many(_ids, fields)
        for stale, fresh in zip(self, results):
            if stale._id is not None:
                stale.merge(fresh)
                stale.clean(fresh.raw.keys())
        return self

    def dump(self, *args, **kwargs):
        return [bizobj.dump(*args, **kwargs) for bizobj in self._bizobj_arr]

    def append(self, bizobj):
        self._perform_on_add([bizobj])
        self._bizobj_arr.append(bizobj)
        return self

    def extend(self, bizobjs):
        self._perform_on_add(bizobjs)
        self._bizobj_arr.extend(bizobjs)
        return self

    def insert(self, index, bizobj):
        self._perform_on_add([bizobj])
        self._bizobj_arr.insert(index, bizobj)
        return self

    def remove(self, bizobj):
        if self.relationship and self.relationship.on_rem:
            if self.relationship.readonly:
                raise RelationshipError(f'{self.relationship} is read-only')
            if bizobj:
                for cb_func in self.relationship.on_rem:
                    cb_func(self.bizobj, bizobj)
            del self._bizobj_arr[self._id.index(bizobj._id)]

    def _perform_on_add(self, bizobjs):
        if self.relationship and self.relationship.on_add:
            if self.relationship.readonly:
                raise RelationshipError(f'{self.relationship} is read-only')
            for bizobj in bizobjs:
                for cb_func in self.relationship.on_add:
                    cb_func(self.bizobj, bizobj)



class BulkPropertyBuilder(object):

    def __init__(self, biz_type, biz_list_type):
        self.biz_type = biz_type
        self.biz_list_type = biz_list_type

    def build_bulk_field_property(self, key):
        return property(
            fget=lambda self: FilterableList(
                getattr(bizobj, key, None)
                for bizobj in self._bizobj_arr
            ),
            fset=lambda self, value: [
                setattr(bizobj, key, value)
                for bizobj in self._bizobj_arr
            ]
        )

    def build_bulk_relationship_property(self, key):
        rel = self.biz_list_type.relationships.get(key)
        use_bulk_relationship = (rel is not None)

        if use_bulk_relationship:
            pass
        else:
            rel = self.biz_type.relationships.get(key)
            if rel is not None:
                return property(
                    fget=lambda self: rel.target.BizList(
                        getattr(bizobj, key, None)
                        for bizobj in self._bizobj_arr
                    ),
                    fset=lambda self, value: [
                        setattr(bizobj, key, value)
                        for bizobj in self._bizobj_arr
                    ]
                )
