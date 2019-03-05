from typing import Type, List
from uuid import UUID

from pybiz.util import repr_biz_id
from pybiz.constants import IS_BIZLIST_ANNOTATION
from pybiz.exc import RelationshipError


class BizList(object):

    @classmethod
    def type_factory(cls, biz_type: Type['BizObject']):
        derived_name = f'{biz_type.__name__}BizList'
        derived_type = type(derived_name, (cls, ), {
            IS_BIZLIST_ANNOTATION: True,
            'biz_type': biz_type,
        })

        def build_property(attr_name):
            return property(
                fget=lambda self: [bizobj[attr_name] for bizobj in self.data]
            )

        for field_name in biz_type.Schema.fields:
            prop = build_property(field_name)
            setattr(derived_type, field_name, prop)

        for rel_name in biz_type.relationships:
            prop = build_property(rel_name)
            setattr(derived_type, rel_name, prop)

        return derived_type

    def __init__(
        self,
        data: List['BizObject'],
        relationship: 'Relationship' = None,
        bizobj: 'BizObject' = None,
    ):
        self.relationship = relationship
        self.data = data or []
        self.bizobj = bizobj

    def __getitem__(self, idx: int) -> 'BizObject':
        return self.data[idx]

    def __len__(self):
        return len(self.data)

    def __bool__(self):
        return bool(self.data)

    def __iter__(self):
        return iter(self.data)

    def __repr__(self):
        id_parts = []
        for bizobj in self.data:
            id_str = repr_biz_id(bizobj)
            dirty_flag = '*' if bizobj.dirty else ''
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
            clone.data += other
        elif isinstance(other, BizList):
            assert self.relationship is other.relationship
            clone.data += other.data
        else:
            raise ValueError(str(other))
        return clone

    def copy(self):
        cls = self.__class__
        return cls(self.data, self.relationship, self.bizobj)

    def save(self, *args, **kwargs):
        return self.biz_type.save_many(self.data, *args, **kwargs)

    def delete(self):
        return self.biz_type.delete_many(
            bizobj._id for bizobj in self.data
            if bizobj.data.get('_id')
        )
        return self

    def dump(self, *args, **kwargs):
        return [bizobj.dump(*args, **kwargs) for bizobj in self.data]

    def append(self, bizobj):
        self.data.append(bizobj)
        self._perform_on_add([bizobj])
        return self

    def extend(self, bizobjs):
        self.data.extend(bizobjs)
        self._perform_on_add(bizobjs)
        return self

    def insert(self, index, bizobj):
        self.data.insert(index, bizobj)
        self._perform_on_add([bizobj])
        return self

    def _perform_on_add(self, bizobjs):
        if not self.relationship.on_add:
            raise RelationshipError(
                f'{self.relationship} must define an on_add callback'
            )
        if self.relationship and self.relationship.on_add:
            for bizobj in bizobjs:
                for cb_func in self.relationship.on_add:
                    cb_func(self.bizobj, bizobj)
