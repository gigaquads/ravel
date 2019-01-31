class Multiset(object):

    @classmethod
    def type_factory(cls, bizobj_type: Type['BizObject']):
        derived_name = f'{bizobj_type.__name__}Multiset'
        derived_type = type(derived_name, (cls, ), {})
        derived_type._bizobj_type = bizobj_type

        for field_name in bizobj_type.schema.fields:
            prop = derived_type.build_field_property(field_name)
            setattr(derived_type, field_name, prop)

        return derived_type

    @classmethod
    def build_field_property(cls, attr_name):
        # TODO: implement dirty interface and cache field props
        return property(
            fget=lambda self: [x[attr_name] for x in self._bizobjs]
        )

    def __init__(self, bizobjs: List['BizObject'] = None):
        self._bizobjs = bizobjs or []

    def __len__(self):
        return len(self._bizobjs)

    def __iter__(self):
        return iter(self._bizobjs)

    def __repr__(self):
        return f'<Multiset({self._bizobj_type.__name__}, size={len(self)})'

    @property
    def data(self):
        return self._bizobjs
