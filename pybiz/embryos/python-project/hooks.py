from appyratus.validation import fields, Schema
from embryo import hooks


class PythonProjectSchema(Schema):
    name = fields.Str()
    description = fields.Str(default='')
    version = fields.Anything()
    tagline = fields.Str()
    action = fields.Str()
    butts = fields.Str(default='wat')


def pre_create(context):
    context = hooks.PreCreateHook.from_schema(PythonProjectSchema, context)
    return context
