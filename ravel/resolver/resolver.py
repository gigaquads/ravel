import sys

from typing import Text, Tuple, List, Set, Dict, Type, Union, Callable
from collections import defaultdict
from copy import deepcopy
from random import randint
from threading import local

from appyratus.utils.dict_utils import DictObject

from ravel.util.loggers import console
from ravel.util.misc_functions import (
    get_class_name,
    flatten_sequence,
)
from ravel.util import (
    is_resource_type, is_batch_type,
    is_resource, is_batch,
)
from ravel.query.order_by import OrderBy
from ravel.query.request import Request
from ravel.query.predicate import (
    Predicate,
    ConditionalPredicate,
    BooleanPredicate,
    OP_CODE,
)

from .resolver_decorator import ResolverDecorator
from .resolver_property import ResolverProperty

class ResolverMeta(type):
    def __init__(cls, name, bases, dict_):
        cls.ravel = DictObject()
        cls.ravel.local = local()
        cls.ravel.local.is_bootstrapped = False



class Resolver(metaclass=ResolverMeta):

    app = None  # TODO: use cls.ravel.app instead

    def __init__(
        self,
        target=None,
        name=None,
        owner=None,
        decorator=None,
        on_resolve=None,
        on_resolve_batch=None,
        on_select=None,
        on_save=None,
        on_set=None,
        on_get=None,
        on_delete=None,
        on_dump=None,
        private=False,
        nullable=True,
        required=False,
        immutable=False,
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
        self.schema = None
        self.many = many
        self.immutable = immutable

        self.on_resolve = on_resolve or self.on_resolve
        self.on_resolve_batch = on_resolve_batch or self.on_resolve_batch
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

    def __repr__(self):
        return (
            f'{get_class_name(self)}('
            f'target={get_class_name(self.owner)}.{self.name}'
            f')'
        )

    @classmethod
    def is_bootstrapped(cls):
        return getattr(cls.ravel.local, 'is_bootstrapped', False)

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

    def copy(self, new_owner: Type['Resource'] = None) -> 'Resolver':
        clone = type(self)(
            target=self.target,
            name=self.name,
            owner=new_owner or self.owner,
            decorator=self.decorator,
            on_resolve=self.on_resolve,
            on_resolve_batch=self.on_resolve_batch,
            on_select=self.on_select,
            on_save=self.on_save,
            on_set=self.on_set,
            on_get=self.on_get,
            on_delete=self.on_delete,
            on_dump=self.on_dump,
            private=self.private,
            nullable=self.nullable,
            required=self.required,
            immutable=self.immutable,
            many=self.many,
        )

        clone.target_callback = self.target_callback
        clone._is_bound = False

        self.on_copy(clone)

        return clone

    @classmethod
    def bootstrap(cls, app: 'Application'):
        console.debug(f'bootstrapping {get_class_name(cls)} class')
        cls.app = app
        cls.on_bootstrap()
        cls.ravel.local.is_bootstrapped = True

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
                raise Exception('unrecognized target class')

        if not self.owner.ravel.is_abstract:
            self.on_bind()

        if self.target is not None:
            self.schema = self.target.Schema(name=self.name)

        self._is_bound = True

    def resolve(self, entity: 'Entity', request=None):
        if request is None:
            request = Request(self)

        if is_resource(entity):
            return self.resolve_resource(entity, request)
        else:
            assert is_batch(entity)
            return self.resolve_batch(entity, request)

    def resolve_resource(self, resource: 'Resource', request):
        self.pre_resolve(resource, request)

        if self.app.mode == 'normal':
            result = self.on_resolve(resource, request)
        elif self.app.mode == 'simulation':
            result = self.on_simulate(resource, request)

        processed_result = self.post_resolve(resource, request, result)
        request.result = processed_result
        return processed_result

    def resolve_batch(self, batch: 'Batch', request):
        self.pre_resolve_batch(batch, request)

        if self.app.mode == 'normal':
            result = self.on_resolve_batch(batch, request)
        elif self.app.mode == 'simulation':
            result = {res: self.on_simulate(res, request) for res in batch}

        processed_result = self.post_resolve_batch(batch, request, result)
        return processed_result

    def generate(self, instance=None, request=None):
        if instance is None:
            instance = self.owner.generate()

        if request is None:
            request = Request(self)

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

    def on_copy(self, copy: 'Resolver'):
        pass

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
        query = request.resolver.target.select(request=request)
        if self.many:
            return query.execute(simulate=True)
        else:
            return query.execute(first=True, simulate=True)

    def pre_resolve_batch(self, batch, request):
        return

    def on_resolve_batch(self, batch, request):
        return None

    def post_resolve_batch(self, batch, request, result):
        return result
