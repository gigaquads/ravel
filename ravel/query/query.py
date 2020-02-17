from typing import Text, Tuple, List, Set, Dict, Type, Union
from copy import deepcopy

from appyratus.utils import DictObject

from ravel.util.loggers import console
from ravel.query.predicate import Predicate
from ravel.util.misc_functions import (
    flatten_sequence,
    get_class_name,
)

from ravel.util import is_resource, is_resource_type
from ravel.resolver.resolver_property import ResolverProperty
from ravel.resolver.resolvers.loader import LoaderProperty, Loader

from .mode import QueryMode
from .order_by import OrderBy
from .request import Request
from .parameters import ParameterAssignment
from .executor import Executor


class Query(object):

    Mode = QueryMode

    def __init__(self, target=None, parent=None, parameters=None, options=None):
        self.target = target
        self.parent = parent
        self.options = options or DictObject(mode=QueryMode.normal)
        self.parameters = parameters or DictObject()
        self.selected = DictObject()
        self.selected.fields = {}
        self.selected.requests = {}

    def __getattr__(self, parameter_name: str):
        return ParameterAssignment(self, parameter_name)

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

    def select(self, *args):
        args = flatten_sequence(args)

        for obj in args:
            if isinstance(obj, str):
                # if obj is str, replace it with the corresponding resolver
                # property from the target Resource class.
                _obj = getattr(self.target, obj, None)
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
            if isinstance(obj, LoaderProperty):
                resolver_property = obj
                request = Request(resolver_property.resolver, query=self)
                self.selected.fields[request.resolver.name] = request
            elif isinstance(obj, ResolverProperty):
                resolver_property = obj
                request = Request(resolver_property.resolver, query=self)
                self.selected.requests[request.resolver.name] = request
            elif isinstance(obj, Request):
                request = obj
                request.query = self
                if isinstance(request.resolver, Loader):
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
                    if order_by_obj.key not in self.target.ravel.resolvers:
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
