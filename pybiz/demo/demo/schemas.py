from appyratus.validation import Schema
from appyratus.validation.fields import Anything, Str, Uuid, Email


class DemoSchema(Schema):
    _id = Anything(load_only=True)
    public_id = Uuid(dump_to='id', default=Uuid.next_uuid)


class UserSchema(DemoSchema):
    name = Str()
    email = Email(allow_none=True)
