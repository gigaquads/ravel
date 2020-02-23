import sys

from typing import Text, Tuple, List, Set, Dict, Type, Union, Callable
from collections import defaultdict
from random import randint

from ravel.util.loggers import console
from ravel.util.misc_functions import (
    get_class_name,
    flatten_sequence,
)
from ravel.util import (
    is_resource, is_batch, is_resource_type, is_batch_type,
)
from ravel.query.order_by import OrderBy
from ravel.query.request import Request
from ravel.query.mode import QueryMode
from ravel.query.predicate import (
    Predicate,
    ConditionalPredicate,
    BooleanPredicate,
    OP_CODE,
)

from .resolver_decorator import ResolverDecorator
from .resolver_property import ResolverProperty


class Resolver(object):

    app = None

    def __init__(
        self,
        name=None,
        owner=None,
        target=None,
        decorator=None,
        on_resolve=None,
        pre_resolve=None,
        post_resolve=None,
        on_select=None,
        on_save=None,
        on_set=None,
        on_get=None,
        on_delete=None,
        on_dump=None,
        private=False,
        nullable=True,
        required=False,
        many=False,
    ):
        self.name = name
        self.owner = owner
        self.private = private
        self.nullable = nullable
        self.decorator = decorator
        self.required = required
        self.target_callback = None
        self.target = None
        self.many = many

        self.on_resolve = on_resolve or self.on_resolve
        self.pre_resolve = pre_resolve or self.pre_resolve
        self.post_resolve = post_resolve or self.post_resolve
        self.on_select = on_select or self.on_select
        self.on_save = on_save or self.on_save
        self.on_dump = on_dump or self.on_dump
        self.on_get = on_get or self.on_get
        self.on_set = on_set or self.on_set
        self.on_delete = on_delete or self.on_delete

        if is_resource_type(target):
            self.target = target
            self.many = False
        elif is_batch(target):
            self.target = target.owner
            self.many = True
        elif target is not None:
            assert callable(target)
            self.target_callback = target

        self._is_bootstrapped = False

    @property
    def is_bootstrapped(self):
        return self._is_bootstrapped

    @classmethod
    def property_type(cls):
        return ResolverProperty

    @classmethod
    def build_property(cls, decorator=None, args=None, kwargs=None):
        args = args or tuple()
        kwargs = kwargs or {}
        resolver = cls(*args, **kwargs)
        property_type = cls.property_type()
        return property_type(resolver, decorator=decorator)

    @classmethod
    def build_decorator(cls, *args, **kwargs):
        class decorator_type(ResolverDecorator):
            def __init__(self, *args, **kwargs):
                super().__init__(cls, *args, **kwargs)

        decorator_type.__name__ = f'{get_class_name(cls)}Decorator'
        return decorator_type

    @classmethod
    def priority(self) -> int:
        """
        Proprity defines the order in which Resolvers execute when executing a
        query, in ascending order.
        """
        return sys.maxsize

    @classmethod
    def tags(cls) -> Set[Text]:
        """
        In development, you can access all Resolvers by tag.  For example, if
        you had a User class with a Resolver called "account" whose class was
        tagged with "my_tag", then you could access this resolver in a
        tag-specific dictionary by doing this:

        ```py
        account_resolver = User.resolvers.my_tag['account']

        # equivalent to doing...
        account_resolver = User.account.resolver
        ```
        """
        return {'untagged'}

    @staticmethod
    def sort(resolvers: List['Resolver']) -> List['Resolver']:
        """
        Sort and return the input resolvers as a new list, orderd by priority
        int, ascending. This reflects the relative order of intended execution,
        from first to last.
        """
        return sorted(resolvers, key=lambda resolver: resolver.priority())

    def bootstrap(cls, app: 'Application'):
        cls.app = app
        cls.on_bootstrap()
        cls._is_bootstrapped = True

    def bind(self):
        if self.target_callback:
            self.app.inject(self.target_callback)
            target = self.target_callback()
            if is_resource_type(target):
                self.target = target
                self.many = False
            elif is_batch_type(target):
                self.target = target.ravel.owner
                self.many = True
            else:
                raise Exception('unrecognized target type')

        self.on_bind()
        self._is_bound = True

    def resolve(self, resource, request):
        self.pre_resolve(resource, request)

        if request.mode == QueryMode.normal:
            result = self.on_resolve(resource, request)
        elif request.mode == QueryMode.backfill:
            resolved_result = self.on_resolve(resource, request)
            result = self.on_backfill(resource, request, resolved_result)
        elif request.mode == QueryMode.simulation:
            result = self.on_simulate(resource, request)

        processed_result = self.post_resolve(resource, request, result)
        return processed_result

    def resolve_batch(self, batch, request):
        self.pre_resolve_batch(batch, request)

        if request.mode == QueryMode.normal:
            result = self.on_resolve_batch(batch, request)
        elif request.mode == QueryMode.backfill:
            resolved_result = self.on_resolve_batch(batch, request)
            result = self.on_backfill_batch(batch, request, resolved_result)
        elif request.mode == QueryMode.simulation:
            result = self.on_simulate_batch(batch, request)

        processed_result = self.post_resolve_batch(batch, request, result)
        return processed_result

    def simulate(self, instance, request):
        self.pre_resolve(instance, request)
        result = self.on_simulate(instance, request)
        processed_result = self.post_resolve(instance, request, result)
        return processed_result

    def dump(self, dumper: 'Dumper', value):
        return value

    @classmethod
    def on_bootstrap(cls):
        pass

    def on_bind(self):
        return

    def pre_resolve(self, resource, request):
        return

    def on_resolve(self, resource, request):
        raise NotImplementedError()

    def post_resolve(self, resource, request, result):
        return result

    def on_select(self, request):
        return

    def on_get(self, resource, value):
        return

    def on_set(self, resource, old_value, new_value):
        return

    def on_delete(self, resource, value):
        return

    def on_dump(self, request, value):
        pass

    def on_save(self, resource, value):
        pass

    def on_simulate(self, resource, request):
        from ravel.query.query import Query

        resolver = request.resolver
        query = Query(request=request)
        select = set(query.requests.keys())

        if self.many:
            count = request.parameters.get('limit', randint(1, 10))
            return self.target.Batch.generate(resolvers=select, count=count)
        else:
            return self.target.generate(resolvers=select)

    def on_backfill(self, resource, request, result):
        raise NotImplementedError()

    def pre_resolve_batch(self, batch, request):
        return

    def on_resolve_batch(self, batch, request):
        return None

    def post_resolve_batch(self, batch, request, result):
        return result

    def on_simulate_batch(self, batch, request):
        raise NotImplementedError()

    def on_backfill_batch(self, batch, request, result):
        raise NotImplementedError()

