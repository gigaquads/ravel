from appyratus.validation import schema, fields


class PythonProjectSchema(schema.Schema):
    name = fields.Str()
    description = fields.Str(default='')
    version = fields.Anything()
    tagline = fields.Str()
    action = fields.Str()
    butts = fields.Str(default='wat')
