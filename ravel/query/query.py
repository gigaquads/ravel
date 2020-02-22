from typing import Text, Tuple, List, Set, Dict, Type, Union, Callable
from collections import defaultdict
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

    def __init__(
        self,
        target: Union[Type['Resource'], Callable] = None,
        parent: 'Query' = None,
        request: 'Reqeust' = None
    ):
        self.target = target
        self.parent = parent
        self.options = DictObject(mode=QueryMode.normal)
        self.requests = {}
        self.parameters = DictObject(
            data={
                'where': None,
                'order_by': None,
                'offset': None,
                'limit': None,
            },
            default=None
        )

        if request:
            if request.resolver and request.resolver.target:
                self.target = request.resolver.target
            if request.parameters:
                params = request.parameters
                if 'select' in params:
                    self.select(params.select)
                if 'where' in params:
                    self.where(params.where)
                if 'order_by' in params:
                    self.order_by(params.order_by)
                if 'limit' in params:
                    self.limit(params.limit)
                if 'offset' in params:
                    self.offset(params.offset)

        if self.target is not None:
            self.select(self.target.ravel.schema.required_fields.keys())
            self.select(self.target.ravel.foreign_keys.keys())

    def __getattr__(self, parameter_name: str):
        return ParameterAssignment(self, parameter_name)

    def __getitem__(self, resolver: Text):
        return self.requests.get(resolver)

    def __len__(self) -> int:
        return len(self.requests)

    def __contains__(self, resolver: Text) -> bool:
        return resolver in self.requests

    def __iter__(self):
        return iter(self.requests)

    def merge(self, other: 'Query', in_place=False) -> 'Query':
        if in_place:
            self.parameters.update(deepcopy(other.parameters))
            self.options.update(deepcopy(other.options))
            for k, v in other.requests.items():
                self.requests[k].update(v)
            return self
        else:
            merged_query = type(self)(
                target=other.target or self.target,
                parent=other.parent or self.parent,
            )
            merged_query.merge(self, in_place=True)
            merged_query.merge(other, in_place=True)
            return merged_query

    def execute(self, first=None):
        if first is not None:
            self.options.first = first

        executor = Executor()
        return executor.execute(self)

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
            request = None
            if isinstance(obj, LoaderProperty) and (obj.decorator is None):
                resolver_property = obj
                request = Request(resolver_property.resolver, query=self)
            elif isinstance(obj, ResolverProperty):
                resolver_property = obj
                request = Request(resolver_property.resolver, query=self)
            elif isinstance(obj, Request):
                request = obj
                request.query = self

            if request:
                self.requests[request.resolver.name] = request

        return self

    def where(self, *predicates, **equality_checks):
        predicates = flatten_sequence(predicates)

        for field_name, value in equality_checks.items():
            resolver_prop = getattr(self.target, field_name)
            pred = (resolver_prop == value)
            predicates.append(pred)

        if predicates:
            if self.parameters.where:
                self.parameters.where &= Predicate.reduce_and(predicates)
            else:
                self.parameters.where = Predicate.reduce_and(predicates)

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
