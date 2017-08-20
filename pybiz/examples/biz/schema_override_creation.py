"""
# Example: `bizobj/schema_override_creation.py`

This file shows you how to specify an external Schema to associate with a
BizObject, as opposed to declaring the fields directly on the BizObject class
itself. Note that BizObject is a subclass of AbstractSchema.

This pattern may come in handy if you want to reuse Schema defined elsewhere. It
can also be useful in fixing circular import errors you might encounter if you
attempt to import Schemas into BizObject modules and vice versa.

"""

from pybiz import BizObject, Schema, fields


class AccountSchema(Schema):

    _id = fields.Int(dump_to='id')
    company_name = fields.Str()


class Account(BizObject):

    @classmethod
    def __schema__(self):
        return AccountSchema


if __name__ == '__main__':

    # create a bizobj
    account = Account(_id=1, company_name='Foo Corp.')

    # marshal out (i.e. validate and transform)
    # the data on its way out.
    print(account.dump())
