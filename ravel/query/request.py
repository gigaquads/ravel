from typing import Text, Dict

from appyratus.utils import DictObject

from ravel.util.misc_functions import get_class_name

from .parameters import ParameterAssignment
from .mode import QueryMode


class Request(object):

    def __init__(self, resolver: 'Resolver', query: 'Query' = None):
        self.resolver = resolver
        self.parameters = DictObject()
        self.query = query
        self.result = None

    def __repr__(self):
        return (
            f'{get_class_name(self)}('
            f'target={get_class_name(self.resolver.owner)}.'
            f'{self.resolver.name}, '
            f'mode={self.mode}'
            f')'
        )

    def __getattr__(self, name: Text) -> 'ParameterAssignment':
        return ParameterAssignment(self, name)

    @property
    def mode(self) -> 'QueryMode':
        if self.query is not None:
            mode = self.query.options.get('mode', QueryMode.normal)
        else:
            mode = QueryMode.normal
        return mode

    @property
    def is_simulated(self) -> bool:
        return self.mode == QueryMode.simulation

    @property
    def is_backfilled(self) -> bool:
        return self.mode == QueryMode.backfill
