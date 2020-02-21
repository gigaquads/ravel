from typing import Dict, Union

from ravel.schema import Schema, Nested
from ravel.resolver.resolver import Resolver
from ravel.resolver.resolvers.loader import Loader, View
from ravel.resolver.resolvers.relationship import Relationship


resolver = Resolver.build_decorator()
relationship = Relationship.build_decorator()
view = View.build_decorator()


class nested(view):
    def __init__(self, schema: Union[Dict, Schema], *args, **kwargs):
        super().__init__(field=Nested(schema), *args, **kwargs)
