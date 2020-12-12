from typing import Dict, Union

from ravel.schema import Schema, fields
from ravel.util.misc_functions import get_callable_name
from ravel.resolver.resolver import Resolver
from ravel.resolver.resolvers.loader import Loader, View
from ravel.resolver.resolvers.relationship import Relationship


resolver = Resolver.build_decorator()
relationship = Relationship.build_decorator()
view = View.build_decorator()


class field:
    """
    This decorator is NOT a ResolverDecorator, but it provides a way that
    fields can be defined on a Resource class while simultaneously defining
    custom resolver logic for loading its value. All uses of this decorator
    will result in a new Field being added to the resource Schema, using the
    name of the decorated method as the name of the field.
    """

    def __init__(self, field: 'Field'):
        self.field = field

    def __call__(self, func):
        self.field.meta['ravel_on_resolve'] = func
        self.field.name = self.field.source = get_callable_name(func)
        return self.field


class nested(view):
    def __init__(self, schema: Union[Dict, Schema], *args, **kwargs):
        super().__init__(field=fields.Nested(schema), *args, **kwargs)
