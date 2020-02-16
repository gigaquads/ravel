import sys

from typing import Text, Tuple, List, Set, Dict, Type, Union, Callable
from collections import defaultdict
from random import randint

from pybiz.util.loggers import console
from pybiz.util.misc_functions import (
    get_class_name,
    flatten_sequence,
)
from pybiz.biz.util import is_resource, is_batch
from pybiz.biz.query.order_by import OrderBy
from pybiz.biz.query.request import Request
from pybiz.biz.query.mode import QueryMode
from pybiz.biz.query.predicate import (
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
        on_resolve=None,
        private=False,
        nullable=True,
        required=False,
        many=False,
    ):
        self.name = name
        self.on_resolve = on_resolve or self.on_resolve
        self.owner = owner
        self.private = private
        self.nullable = nullable
        self.required = required
        self.target_callback = None
        self.target = None
        self.many = many

        if is_resource(target):
            assert isinstance(target, type)
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
    def build_property(cls, *args, **kwargs):
        resolver = cls(*args, **kwargs)
        property_type = cls.property_type()
        return property_type(resolver)

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
            if is_resource(target):
                assert isinstance(target, type)
                self.target = target
                self.many = False
            elif is_batch(target):
                self.target = target.owner
                self.many = True

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

    def simulate(self, instance, request):
        self.pre_resolve(instance, request)
        result = self.on_simulate(instance, request)
        processed_result = self.post_resolve(instance, request, result)
        return processed_result

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

    def on_simulate(self, resource, request):
        resolver = request.resolver
        if self.many:
            count = request.parameters.get('limit', randint(1, 10))
            return self.target.Batch.generate(count)
        else:
            return self.target.generate()

    def on_backfill(self, resource, request, result):
        raise NotImplementedError()
