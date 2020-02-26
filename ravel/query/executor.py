from typing import List, Set, Text, Dict

from ravel.util.loggers import console
from ravel.batch import Batch
from ravel.constants import ID, REV


class Executor(object):
    def execute(self, query: 'Query') -> 'Entity':
        # extract information needed to perform execution logic
        info = self._analyze_query(query)

        # fetch target fields from the DAL and execute all other requested
        # resolvers that don't correspond to fields, merging the results into
        # the fetched resources.
        resources = self._fetch_resources(query, info['fields'])
        self._execute_requests(query, resources, info['requests'])
        return resources

    def _analyze_query(self, query) -> Dict:
        fields_to_fetch = {ID, REV}
        requests_to_execute = set()
        schema = query.target.ravel.schema
        for request in query.requests.values():
            if request.resolver.name in schema.fields:
                fields_to_fetch.add(request.resolver.name)
            else:
                requests_to_execute.add(request)
        return {
            'fields': fields_to_fetch,
            'requests': requests_to_execute,
        }

    def _fetch_resources(
        self,
        query: 'Query',
        fields: Set[Text],
    ) -> List['Resource']:
        """
        # Fetch Resources
        This is where we take the query params and send them to the store
        """
        resource_type = query.target
        store = resource_type.ravel.store
        where_predicate = query.parameters.where
        kwargs = query.parameters.to_dict()
        records = store.query(where_predicate, fields=fields, **kwargs)
        return resource_type.Batch(
            resource_type(state=record).clean()
            for record in records
        )

    def _execute_requests(self, query, resources, requests):
        for request in requests:
            resolver = request.resolver
            # first try resolving in batch
            if len(resources) > 1:
                results = resolver.resolve_batch(resources, request)
                if results:
                    for resource in resources:
                        value = results.get(resource)
                        if self.many and value is None:
                            value = self.target.Batch()
                        elif (not self.many) and is_batch(value):
                            value = None
                        resource.internal.state[resolver.name] = value
                    return

            # default on resolving for each resource separatesly
            for resource in resources:
                value = resolver.resolve(resource, request)
                resource.internal.state[resolver.name] = value
