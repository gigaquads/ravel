from typing import Text, Dict

from appyratus.utils import DictObject

from ravel.util.misc_functions import get_class_name

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
            f'{self.resolver.name}, '
            f'result={bool(self.result)}, '
            f'query={self.query}'
            f')'
        )

    def __getattr__(self, name: Text) -> 'ParameterAssignment':
        return ParameterAssignment(self, name)

    def select(self, *args, **kwargs) -> 'Request':
        if args:
            self.parameters.select.extend(args)
        if kwargs:
            self.parameters.select.append(kwargs)
        return self

    def to_query(self, **query_kwargs) -> 'Query':
        # NOTE: the Query class must be available in the global namespace.
        # it is not imported here to avoid cyclic import error and also
        # to avoid the uglyness of a lexically-scoped import.
        return Request.Query(request=self, **query_kwargs)
