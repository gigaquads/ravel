from ravel import Resource, fields


class Thing(Resource):
    anything = fields.Field()
    name = fields.String()
    option = fields.Enum(fields.String(), {'a', 'b', 'c'})
    code = fields.Bytes()
    ident = fields.UuidString()
    when = fields.DateTimeString()
    age = fields.Int()
    email = fields.Email()
    real = fields.Float()
    key = fields.Uuid()
    happy = fields.Bool()
    dob = fields.DateTime()
    timestamp = fields.Timestamp()
    colors = fields.List(fields.String())
    reals = fields.List(fields.Float())
    integers = fields.Set(fields.Int())
    blob = fields.Dict()
    nested = fields.Nested({
        'email': fields.Email(),
        'substrings': fields.List(fields.String()),
        'subdicts': fields.List(fields.Nested({
            'age': fields.Int(),
            'name': fields.String(),
        }))
    })
