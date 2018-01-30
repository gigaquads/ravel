from appyratus import schema

from embryo import hooks


class PythonProjectSchema(schema.Schema):
    name = schema.Str()
    description = schema.Str(default='')
    version = schema.Anything()
    tagline = schema.Str()
    action = schema.Str()
    butts = schema.Str(default='wat')


def pre_create(context):
    context = hooks.PreCreateHook.from_schema(PythonProjectSchema, context)
    return context
