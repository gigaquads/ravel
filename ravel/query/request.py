from typing import Text, Dict

from appyratus.utils.dict_utils import DictObject

from ravel.util import is_resource, is_resource_type
from ravel.util.misc_functions import get_class_name, flatten_sequence

from .parameters import ParameterAssignment


class Request(object):

    # Query is set at runtime as a side-effect of
    # importing the Query module.
    Query = None


    def __init__(
        self,
        resolver: 'Resolver',
        query: 'Query' = None,
        parent: 'Request' = None,
    ):
        self.resolver = resolver
        self.parameters = DictObject({'select': []})
        self.query = query
        self.result = None
        self.parent = parent

    def __repr__(self):
        return (
            f'{get_class_name(self)}('
            f'target={get_class_name(self.resolver.owner)}.'
            f'{self.resolver.name}'
            f')'
        )

    def __getattr__(self, name: Text) -> 'ParameterAssignment':
        return ParameterAssignment(self, name)

    def select(self, *args) -> 'Request':
        from ravel.resolver.resolvers.loader import LoaderProperty
        from ravel.resolver.resolver_property import ResolverProperty

        args = flatten_sequence(args)

        for obj in args:
            if isinstance(obj, str):
                # if obj is str, replace it with the corresponding resolver
                # property from the target Resource class.
                _obj = getattr(self.resolver.owner, obj, None)
                if _obj is None:
                    raise ValueError(f'unknown resolver: {obj}')
                obj = _obj
            elif is_resource(obj):
                self.select(obj.internal.state.keys())
                continue
            elif is_resource_type(obj):
                self.select(obj.ravel.resolvers.keys())
                continue

            # build a resolver request
            request = None
            if isinstance(obj, LoaderProperty) and (obj.decorator is None):
                resolver_property = obj
                request = Request(resolver_property.resolver)
            elif isinstance(obj, ResolverProperty):
                resolver_property = obj
                request = Request(resolver_property.resolver)
            elif isinstance(obj, Request):
                request = obj

            if request is not None:
                self.parameters.select.append(request)
            elif obj is not None:
                self.parameters.select.append(obj)

        return self