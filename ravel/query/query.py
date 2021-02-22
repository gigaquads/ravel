from typing import Text, List, Dict, Type, Union, Callable
from copy import deepcopy

from appyratus.utils.dict_utils import DictObject

from ravel.util.loggers import console
from ravel.query.predicate import Predicate
from ravel.util.misc_functions import (
    flatten_sequence,
    get_class_name,
)

from ravel.util import is_resource, is_resource_type
from ravel.resolver.resolver_property import ResolverProperty
from ravel.resolver.resolvers.loader import LoaderProperty

from .order_by import OrderBy
from .request import Request
from .parameters import ParameterAssignment
from .executor import Executor


class Query(object):
    def __init__(
        self,
        target: Union[Type['Resource'], Callable] = None,
        sources: List['Resource'] = None,
        parent: 'Query' = None,
        request: 'Request' = None,
        eager: bool = True,
    ):
        self.sources = sources or []
        self.target = target
        self.parent = parent
        self.options = DictObject()
        self.from_request = request
        self.eager = eager
        self.requests = {}
        self.callbacks = []
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
            self.merge(request, in_place=True)

        if self.target is not None:
            self.select(self.target.ravel.schema.required_fields.keys())
            self.select(self.target.ravel.foreign_keys.keys())

    def __getattr__(self, parameter_name: str):
        return ParameterAssignment(self, parameter_name)

    def __getitem__(self, resolver: Text):
        return self.requests.get(resolver)

    def __call__(self, *args, **kwargs):
        return self.execute(*args, **kwargs)

    def __len__(self) -> int:
        return len(self.requests)

    def __contains__(self, resolver: Text) -> bool:
        return resolver in self.requests

    def __iter__(self):
        return iter(self.execute())

    def __repr__(self):
        offset = self.parameters.offest
        limit = self.parameters.limit
        return (
            f'Query('
            f'target={get_class_name(self.target)}['
            f'{offset if offset is not None else ""}'
            f':'
            f'{limit if limit is not None else ""}'
            f'])'
        )

    def merge(
        self,
        other: Union['Query', 'Request'],
        in_place: bool = False
    ) -> 'Query':
        if in_place:
            if isinstance(other, Query):
                self.parameters.update(deepcopy(other.parameters.to_dict()))
                self.options.update(deepcopy(other.options.to_dict()))
                self.select(other.requests.values())
            else:
                assert isinstance(other, Request)
                self._merge_request(other)
            return self
        else:
            merged_query = type(self)(
                target=self.target,
                parent=self.parent,
            )
            merged_query.merge(self, in_place=True)
            if isinstance(other, Query):
                merged_query.merge(other, in_place=True)
            else:
                assert isinstance(other, Request)
                merged_query._merge_request(other)
            return merged_query

    def _merge_request(self, request: 'Request'):
        # set the target type or ensure that the request's resolver
        # target type is the same as this query's, or bail.
        if request.resolver and request.resolver.target:
            if self.target is None:
                self.target = request.resolver.target
            elif self.target is not request.resolver.target:
                raise Exception(
                    'cannot merge two queries with different target types'
                )
        # marshal in raw query parameters from the request
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

    def configure(
        self,
        options: Dict = None,
        **more_options
    ) -> 'Query':
        options = dict(options or {}, **more_options)
        self.options.update(options)
        return self

    def execute(
        self,
        first=None,
        simulate=False,
    ) -> Union['Resource', 'Batch']:
        """
        Execute the query, returning a single Resource ora a Batch.
        """

        if self.eager:
            # "eager" here means that we always fetch at least ALL field
            # values by adding them to the "select" set...
            self.select(self.target.ravel.resolvers.fields.keys())

        executor = Executor(simulate=simulate)
        batch = executor.execute(self, sources=self.sources)

        if first:
            result = batch[0] if batch else None
        else:
            result = batch

        if self.callbacks:
            for func in self.callbacks:
                func(self, result)

        return result

    def exists(self):
        self.requests.clear()
        self.select(self.target._id)
        return bool(self.execute(first=True))

    def deselect(self, *args):
        """
        Remove the given arguments from the query's requests dict.
        """
        args = flatten_sequence(args)
        keys = set()

        for obj in args:
            if isinstance(obj, str):
                # if obj is str, replace it with the corresponding resolver
                # property from the target Resource class.
                _obj = getattr(self.target, obj, None)
                if _obj is None:
                    raise ValueError(f'unknown resolver: {obj}')
                obj = _obj
            elif is_resource(obj):
                keys.update(obj.internal.state.keys())
                continue
            elif is_resource_type(obj):
                keys.update(obj.ravel.resolvers.keys())
                continue

            # build a resolver request
            request = None
            if isinstance(obj, LoaderProperty) and (obj.decorator is None):
                resolver_property = obj
                keys.add(resolver_property.resolver.name)
            elif isinstance(obj, ResolverProperty):
                resolver_property = obj
                keys.add(resolver_property.resolver.name)
            elif isinstance(obj, Request):
                request = obj
                keys.add(request.resolver.name)

        for key in keys:
            if key in self.requests:
                del self.requests[key]

        return self

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
                request = Request(resolver_property.resolver)
            elif isinstance(obj, ResolverProperty):
                resolver_property = obj
                request = Request(resolver_property.resolver)
            elif isinstance(obj, Request):
                request = obj

            # finally set the request if one was generated
            if request:
                resolver_name = request.resolver.name
                if resolver_name not in self.target.ravel.virtual_fields:
                    if self.from_request is not None:
                        request.parent = self.from_request
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
