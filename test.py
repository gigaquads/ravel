from pybiz import BizObject, Relationship, fields

class Base(BizObject):
    base_field = fields.Anything()

class Sibling(Base):
    sibling_derived_field = fields.Anything()

class Derived(Base):
    derived_field = fields.Anything()
    sibling = Relationship(Sibling)


print(Derived.Schema.fields.keys())

derived = Derived(
    derived_field='derived_field',
    sibling=Sibling(sibling_derived_field='sibling_derived_field'))
    
print(derived.sibling.Schema.fields)
derived.sibling.sibling_derived_field = 'haha!'

print(derived)
print(derived.dump())
