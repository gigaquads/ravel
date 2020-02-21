from typing import List

from ravel.util.loggers import console
from ravel.batch import Batch
from ravel.constants import ID, REV


class Executor(object):
    def execute(self, query: 'Query') -> 'Entity':
        resources = self._fetch_resources(query)
        self._execute_resolvers(query, resources)
        retval = resources
        if query.options.first:
            retval = resources[0] if resources else None
        return retval

    def _fetch_resources(self, query: 'Query') -> List['Resource']:
        store = query.target.ravel.store
        where_predicate = query.parameters.where
        field_names = list(
            query.selected.fields.keys() | {ID, REV}
        )

        kwargs = query.parameters.to_dict()
        if 'where' in kwargs:
            del kwargs['where']

        state_dicts = store.query(
            predicate=where_predicate,
            fields=field_names,
            **kwargs
        )

        resource_type = query.target
        return resource_type.Batch(
            resource_type(state=state).clean()
            for state in state_dicts
        )

    def _execute_resolvers(self, query, resources):
        for request in query.selected.requests.values():
            resolver = request.resolver
            for resource in resources:
                value = resolver.resolve(resource, request)
                resource.internal.state[resolver.name] = value
