"""
# Example: `bizobj/basic_creation.py`

This file demonstrates how to define new BizObject classes.

"""

from pybiz import BizObject, Relationship, fields


class Account(BizObject):

    _id = fields.Int(dump_to='id')
    company_name = fields.Str()


class User(BizObject):

    _id = fields.Int(dump_to='id')
    account = Relationship(Account)
    name = fields.Str()
    age = fields.Int()


if __name__ == '__main__':

    # create some bizobjs
    foo_corp = Account(company_name='Foo Corp.')
    bob = User(name='Bobbert', age=500, account=foo_corp)

    # update some fields on them
    bob.account.company_name = 'Foo Corp. LLC'
    bob.age = 501

    assert bob.dirty
    assert bob.account.dirty
    # ^ these asserts would fail after save() is called

    # marshal out (i.e. validate and transform)
    # the data on its way out.
    print(bob.dump())
