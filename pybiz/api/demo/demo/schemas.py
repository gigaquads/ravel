from appyratus.validation import Schema
from appyratus.validation.fields import Anything, Str, Uuid, Email


class UserSchema(Schema):
    _id = Uuid(dump_to='id')
    email = Email(allow_none=True)
    name = Str()
