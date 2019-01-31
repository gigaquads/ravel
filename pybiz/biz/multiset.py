from typing import Type, List


class Multiset(object):

    @classmethod
    def type_factory(cls, bizobj_type: Type['BizObject']):
        derived_name = f'{bizobj_type.__name__}Multiset'
        derived_type = type(derived_name, (cls, ), {})
        derived_type._bizobj_type = bizobj_type

        # TODO: implement dirty interface and cache field props
        def build_property(attr_name):
            return property(
                fget=lambda self: [x[attr_name] for x in self._bizobjs]
            )

        for field_name in bizobj_type.schema.fields:
            prop = build_property(field_name)
            setattr(derived_type, field_name, prop)

        for rel_name in bizobj_type.relationships:
            prop = build_property(rel_name)
            setattr(derived_type, rel_name, prop)

        return derived_type

    def __init__(self, bizobjs: List['BizObject'] = None):
        self._bizobjs = bizobjs or []

    def __len__(self):
        return len(self._bizobjs)

    def __iter__(self):
        return iter(self._bizobjs)

    def __repr__(self):
        id_parts = []
        for bizobj in self._bizobjs:
            id_parts.append('{}{}'.format(
                bizobj._id or '?',
                '*' if bizobj.dirty else ''
            ))
        ids = ', '.join(id_parts)
        return (
            f'<Multiset(type={self._bizobj_type.__name__}, '
            f'size={len(self)}, ids=[{ids}])>'
        )

    @property
    def data(self):
        return self._bizobjs

    def save(self, *args, **kwargs):
        for bizobj in self._bizobjs:
            bizobj.save(*args, **kwargs)
        return self

    def delete(self):
        return self._bizobj_type.delete_many(
            bizobj._id for bizobj in self._bizobjs
            if bizobj.data.get('_id')
        )
        return self

    def dump(self, *args, **kwargs):
        return [bizobj.dump(*args, **kwargs) for bizobj in self._bizobjs]
