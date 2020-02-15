from typing import Text, Tuple, List, Set, Dict, Type, Union
from copy import deepcopy

from appyratus.utils import DictObject

from pybiz.util.loggers import console
from pybiz.predicate import Predicate
from pybiz.util.misc_functions import (
    get_class_name,
    flatten_sequence,
)

from .util import is_batch, is_resource
from .dirty import DirtyDict
from .entity import Entity
from .order_by import OrderBy
from .resolver import (
    ResolverProperty, EagerStoreLoaderProperty, EagerStoreLoader
)


class Query(object):
    def __init__(self, target=None, parent=None, parameters=None, options=None):
        self.target = target
        self.parent = parent
        self.options = options or DictObject()
        self.parameters = parameters or DictObject()
        self.selected = DictObject()
        self.selected.fields = {}
        self.selected.requests = {}

    def __getattr__(self, parameter_name: str):
        return ParameterAssignment(parameter_name, self)

    def execute(self, first=None):
        if first is not None:
            self.options.first = first

        executor = Executor()
        return executor.execute(self)

    def merge(self, other: 'Query', in_place=False) -> 'Query':
        if in_place:
            self.parameters.update(deepcopy(other.parameters))
            self.options.update(deepcopy(other.options))
            self.selected.fields.update(deepcopy(other.selected.fields))
            self.selected.requests.update(deepcopy(other.selected.requests))
            return self
        else:
            merged_query = type(self)(
                target=other.target or self.target,
                parent=other.parent or self.parent,
            )
            merged_query.merge(self, in_place=True)
            merged_query.merge(other, in_place=True)
            return merged_query

    def select(self, *selectors):
        selectors = flatten_sequence(selectors)

        for obj in selectors:
            if isinstance(obj, str):
                # if obj is str, replace it with the corresponding resolver
                # property from the target Resource class.
                _obj = getattr(self.target, obj, None)
                if _obj is None:
                    raise ValueError(f'unknown resolver: {obj}')
                obj = _obj

            # insert a Request object into self.selected
            if isinstance(obj, EagerStoreLoaderProperty):
                resolver_property = obj
                request = Request(resolver_property.resolver)
                self.selected.fields[request.resolver.name] = request
            elif isinstance(obj, ResolverProperty):
                resolver_property = obj
                request = Request(resolver_property.resolver)
                self.selected.requests[request.resolver.name] = request
            elif isinstance(obj, Request):
                request = obj
                if isinstance(request.resolver, EagerStoreLoader):
                    self.selected.fields[request.resolver.name] = request
                else:
                    self.selected.requests[request.resolver.name] = request

        return self

    def where(self, *predicates, append=True):
        predicates = flatten_sequence(predicates)
        if predicates:
            if self.parameters.where:
                self.parameters.where &= Predicate.reduce_and(predicates)
            else:
                self.parameters.where = Predicate.reduce_and(predicates)
        elif not append:
            self.parameters.where = None
        return self

    def order_by(self, *order_by):
        order_by = flatten_sequence(order_by)

        if order_by:
            self.parameters.order_by = []

            for obj in order_by:
                if isinstance(obj, OrderBy):
                    self.parameters.order_by.append(obj)
                elif isinstance(obj, str):
                    if obj.lower().endswith(' desc'):
                        order_by_obj = OrderBy(obj.split()[0], desc=True)
                    else:
                        order_by_obj = OrderBy(obj.split()[0], desc=False)
                    if order_by_obj.key not in self.target.pybiz.resolvers:
                        raise ValueError(
                            f'uncognized resolver: {order_by_obj.key}'
                        )
                    self.parameters.order_by.append(order_by_obj)
        else:
            self.parameters.order_by = None

        return self

    def offset(self, offset=None):
        if offset is not None:
            self.parameters.offset = max(0, int(offset))
        else:
            self.parameters.offset = None
        return self

    def limit(self, limit):
        if limit is not None:
            self.parameters.limit = max(1, int(limit))
        else:
            self.parameters.limit = None
        return self


class Executor(object):
    def execute(self, query):
        resources = self._fetch_resources(query)

        self._execute_resolvers(query, resources)

        retval = resources
        if query.options.first:
            retval = resources[0] if resources else None

        return retval

    def _fetch_resources(self, query):
        store = query.target.pybiz.store
        where_predicate = query.parameters.where
        field_names = [req.resolver.field.name for req in query.selected.fields]
        state = store.query(predicate=where_predicate, fields=field_names)
        return [query.target(s).clean() for s in state]

    def _execute_resolvers(self, query, resources):
        for request in query.selected.requests:
            resolver = request.resolver
            for resource in resources:
                value = resolver.resolve(resource, request)
                setattr(resource, resolver.name, value)




class Request(object):
    def __init__(self, resolver):
        self.resolver = resolver
        self.parameters = DictObject()
        self.result = None

    def __repr__(self):
        return (
            f'{get_class_name(self)}('
            f'{get_class_name(self.resolver.owner)}.'
            f'{self.resolver.name}'
            f')'
        )

    def __getattr__(self, name) -> 'ParameterAssignment':
        return ParameterAssignment(name, self)


class ParameterAssignment(object):
    """
    This is an internal data structure, used to facilitate the syntactic sugar
    that allows you to write to query.params via funcion call notation, like
    query.foo('bar') instead of query.params['foo'] = bar.

    Instances of this class just store the query whose parameter we are going to
    set and the name of the dict key or "param name". When called, it writes the
    single argument supplied in the call to the params dict of the query.
    """

    def __init__(self, name, query=None, parameters=None):
        self._parameters = parameters
        self._name = name

    def __call__(self, value):
        """
        Store the `param` value in the Query's parameters dict.
        """
        self._parameters[self._name] = value
        return self._query

    def __repr__(self):
        return (
            f'{get_class_name(self)}('
            f'parameter={self._name}'
            f')'
        )
