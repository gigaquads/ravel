from pybiz import Schema, fields


class CreateUserSchema(Schema):

    name = fields.Str()
    email = fields.Str()


class UserSchema(Schema):

    # NOTE: We aren't using a public ID in this example.
    # For simplicity's sake, _id is itself used publically
    # in the example API. Not recommended in real life.

    _id = fields.Str(dump_to='id')
    public_id = fields.Str(load_only=True)
    name = fields.Str()
    email = fields.Str()
