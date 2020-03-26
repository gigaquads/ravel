from ravel.util import get_class_name
from ravel.schema import Schema


def get_stripped_schema_name(obj):
    if isinstance(obj, Schema):
        name = get_class_name(obj)
    else:
        assert isinstance(obj, str)
        name = obj
    if name.endswith('Schema'):
        return name[:-len('Schema')]
    else:
        return name