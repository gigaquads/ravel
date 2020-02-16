from typing import List
from pybiz.util.loggers import console



class Executor(object):
    def execute(self, query: 'Query') -> 'Entity':
        resources = self._fetch_resources(query)
        self._execute_resolvers(query, resources)
        retval = resources
        if query.options.first:
            retval = resources[0] if resources else None
        return retval

    def _fetch_resources(self, query: 'Query') -> List['Resource']:
        store = query.target.pybiz.store
        where_predicate = query.parameters.where
        field_names = [req.resolver.field.name for req in query.selected.fields]
        state_dicts = store.query(predicate=where_predicate, fields=field_names)
        return Batch(query.target(s).clean() for s in state_dicts)

    def _execute_resolvers(self, query, resources):
        for request in query.selected.requests:
            resolver = request.resolver
            for resource in resources:
                value = resolver.resolve(resource, request)
                setattr(resource, resolver.name, value)
