import sys
import re

from copy import deepcopy
from typing import Text, Set, Dict, List
from collections import defaultdict

import pybiz.biz

from pybiz.util.loggers import console
from pybiz.util.misc_functions import (
    is_sequence,
    flatten_sequence,
)
from pybiz.predicate import (
    Predicate,
    ConditionalPredicate,
    BooleanPredicate,
    OP_CODE,
)

from .biz_thing import BizThing
from .util import is_biz_object


class Resolver(object):
    def __init__(
        self,
        biz_class=None,
        name=None,
        schema=None,
        lazy=True,
        on_execute=None,
        on_post_execute=None,
        on_get=None,
        on_set=None,
        on_del=None,
    ):
        self._schema = schema
        self._name = name
        self._lazy = lazy
        self._biz_class = biz_class
        self._is_bootstrapped = False
        self._is_bound = False

        self.on_execute = on_execute or self.on_execute
        self.on_post_execute = on_post_execute or self.on_post_execute
        self.on_get = on_get or self.on_set
        self.on_set = on_set or self.on_set
        self.on_del = on_del or self.on_del

    def __repr__(self):
        return (
            f'<Resolver('
            f'name="{self.name}", '
            f'tags={"|".join(self.tags())}, '
            f'priority={self.priority()}'
            f'>'
        )

    @classmethod
    def bootstrap(cls, biz_class):
        cls.on_bootstrap()
        cls._is_bootstrapped = True

    @classmethod
    def on_bootstrap(cls):
        pass

    @property
    def is_bootstrapped(self):
        return self._is_bootstrapped

    def bind(self, biz_class):
        self.on_bind(biz_class)
        self._is_bound = True

    def on_bind(self, biz_class):
        pass

    @property
    def is_bound(self):
        return self._is_bound

    @property
    def name(self):
        return self._name

    @property
    def lazy(self):
        return self._lazy

    @property
    def schema(self):
        return self._schema

    @property
    def biz_class(self):
        return self._biz_class

    @biz_class.setter
    def biz_class(self, biz_class):
        self._biz_class = biz_class

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

    def copy(self, unbind=True) -> 'Resolver':
        """
        Return a copy of this resolver. If unbind is set, clear away the
        reference to the BizObject class to which this resolver is bound.
        """
        clone = deepcopy(self)
        if unbind:
            clone.biz_class = None
            clone._is_bound = False
        return clone

    def execute(self, instance, query=None, *args, **kwargs):
        """
        Return the result of calling the on_execute callback. If self.state
        is set, then we return any state data that may exist, in which case
        no new call is made.
        """
        if self.on_execute is not None:
            if self.name not in instance.internal.state:
                result = self.on_execute(instance, self, *args, **kwargs)
                instance.internal.state[self.name] = result
            else:
                result = instance.internal.state[self.name]
            result = self.on_post_execute(instance, self, result, query)
            return result
        else:
            console.warning(
                message=(
                    f'useless execution of {self}. '
                    f'on_execute callback not defined',
                ),
                data={
                    'instance': instance
                }
            )
            return None

    def dump(self, dumper: 'Dumper', value):
        raise NotImplementedError()

    def generate(
        self,
        instance: 'BizObject',
        query: 'ResolverQuery' = None,
        *args, **kwargs
    ):
        raise NotImplementedError()

    @staticmethod
    def on_execute(
        instance: 'BizObject',
        resolver: 'Resolver',
        query: 'Query' = None,
        *args,
        **kwargs
    ):
        raise NotImplementedError()

    @staticmethod
    def on_post_execute(
        instance: 'BizObject',
        resolver: 'Resolver',
        result: object,
        query: 'Query' = None,
    ):
        return result

    @staticmethod
    def on_get(resolver: 'Resolver', value):
        return None

    @staticmethod
    def on_set(resolver: 'Resolver', old_value, value):
        return

    @staticmethod
    def on_del(resolver: 'Resolver', deleted_value):
        return

    @staticmethod
    def on_save(resolver: 'Resolver', owner: 'BizObject', value) -> 'BizThing':
        return value if isinstance(value, BizThing) else None


class ResolverManager(object):
    def __init__(self):
        self._resolvers = {}
        self._tag_2_resolvers = defaultdict(dict)

    def __getattr__(self, tag):
        return self.by_tag(tag)

    def __getitem__(self, resolver_name):
        return self._resolvers.get(resolver_name)

    def __setitem__(self, resolver_name, resolver):
        assert resolver_name == resolver.name
        old_resolver = self._resolvers.get(resolver_name)
        if old_resolver is not None:
            del self._resolvers[resolver_name]
            for tag in old_resolver.tags():
                del self._tag_2_resolvers[tag][resolver_name]

        self._resolvers[resolver_name] = resolver
        for tag in resolver.tags():
            self._tag_2_resolvers[tag][resolver_name] = resolver

    def __iter__(self):
        return iter(self._resolvers)

    def __contains__(self, obj):
        if isinstance(obj, Resolver):
            return obj.name in self._resolvers
        else:
            return obj in self._resolvers

    def __len__(self):
        return len(self._resolvers)

    def keys(self):
        return list(self._resolvers.keys())

    def values(self):
        return list(self._resolvers.values())

    def items(self):
        return list(self._resolvers.items())

    def register(self, resolver):
        self._resolvers[resolver.name] = resolver
        for tag in resolver.tags():
            self._tag_2_resolvers[tag][resolver.name] = resolver

    def by_tag(self, tag, invert=False):
        if not invert:
            return self._tag_2_resolvers.get(tag, {})
        else:
            resolvers = {}
            keys_to_exclude = self._tag_2_resolvers.get(tag, {}).keys()
            for tag_key, resolver_dict in self._tag_2_resolvers.items():
                if tag_key != tag:
                    resolvers.update(resolver_dict)
            return resolvers


class FieldResolver(Resolver):
    def __init__(self, field, lazy=True, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._field = field
        self._lazy = lazy

    @property
    def asc(self) -> 'OrderBy':
        return pybiz.biz.OrderBy(self.field.source, desc=False)

    @property
    def desc(self) -> 'OrderBy':
        return pybiz.biz.OrderBy(self.field.source, desc=True)


    @property
    def lazy(self):
        return self._lazy

    @property
    def field(self):
        return self._field

    @classmethod
    def tags(cls):
        return {'fields'}

    @classmethod
    def priority(cls):
        return 1

    def on_execute(self, instance, query=None):
        key = self._field.name
        is_value_loaded = key in instance.internal.state
        if self._lazy and (not is_value_loaded):
            unloaded_field_names = (
                instance.Schema.fields.keys() - instance.internal.state.keys()
            )
            instance.load(unloaded_field_names)
        value = instance.internal.state.get(key)
        return value

    def on_select(self, query: 'ResolverQuery', selectors):
        pass

    def dump(self, dumper: 'Dumper', value):
        processed_value, errors = self._field.process(value)
        if errors:
            raise Exception('ValidationError: ' + str(errors))
        return processed_value


class ResolverProperty(property):
    """
    All Pybiz-aware attributes at the BizObject class level are instances of
    this class, including field properties, like `User.name`.
    """

    def __init__(self, resolver: Resolver):
        self.resolver = resolver
        self._hash = hash(self.biz_class) + int(
            re.sub(r'[^a-zA-Z0-9]', '', self.resolver.name), 36
        )
        super().__init__(
            fget=self._fget,
            fset=self._fset,
            fdel=self._fdel,
        )

    def __hash__(self):
        return self._hash

    @property
    def biz_class(self):
        return self.resolver.biz_class if self.resolver else None

    def select(self, *selectors, append=True):
        from pybiz.biz2.query import ResolverQuery

        query = ResolverQuery(self.resolver)
        query.select(selectors, append=append)
        self.resolver.on_select(query, selectors)

        return query

    def _fget(self, instance):
        key = self.resolver.name
        obj = self.resolver.execute(instance, query=None)
        if self.resolver.on_get:
            self.resolver.on_get(instance, resolver, obj)
        return obj

    def _fset(self, instance, obj):
        key = self.resolver.name
        old_obj = instance.internal.state.pop(key, None)
        instance.internal.state[key] = obj
        if self.resolver.on_set:
            self.resolver.on_set(resolver, old_obj, obj)

    def _fdel(self, instance):
        key = self.resolver.name
        obj = instance.internal.state.pop(key, None)
        if self.resolver.on_del:
            self.resolver.on_del(resolver, obj)


class FieldResolverProperty(ResolverProperty):

    def __hash__(self):
        return super().__hash__()

    def __eq__(self, other: Predicate) -> Predicate:
        return ConditionalPredicate(OP_CODE.EQ, self, other)

    def __ne__(self, other: Predicate) -> Predicate:
        return ConditionalPredicate(OP_CODE.NEQ, self, other)

    def __lt__(self, other: Predicate) -> Predicate:
        return ConditionalPredicate(OP_CODE.LT, self, other)

    def __le__(self, other: Predicate) -> Predicate:
        return ConditionalPredicate(OP_CODE.LEQ, self, other)

    def __gt__(self, other: Predicate) -> Predicate:
        return ConditionalPredicate(OP_CODE.GT, self, other)

    def __ge__(self, other: Predicate) -> Predicate:
        return ConditionalPredicate(OP_CODE.GEQ, self, other)

    def including(self, *others) -> Predicate:
        others = flatten_sequence(others)
        others = {obj._id if is_biz_object(obj) else obj for obj in others}
        return ConditionalPredicate(OP_CODE.INCLUDING, self, others)

    def excluding(self, *others) -> Predicate:
        others = flatten_sequence(others)
        others = {obj._id if is_biz_object(obj) else obj for obj in others}
        return ConditionalPredicate(OP_CODE.EXCLUDING, self, others)

    @property
    def field(self):
        return self.resolver.field


class ResolverDecorator(object):
    """
    This is the "@resolver" in code, like:

    ```py
    @resolver
    def account(self, resolver):
        return Account.select().where(_id=self.account_id).execute(first=True)

    @account.on_get
    def log_account_access(self, resolver, account):
        log_access(self, account)
    ```

    The job of the decorator is to collect together all arguments required by
    the constructor of the Resolver type (self.resolver_class).
    """

    def __init__(self, resolver: Resolver = None, schema=None, **kwargs):
        if not isinstance(resolver, type):
            # in this case, the decorator was used like "@resolver"
            self.resolver_class = Resolver
            self.on_execute_func = resolver
            self.name = resolver.__name__
        else:
            # otherwise, it was used like "@resolver()"
            self.resolver_class = resolver or Resolver
            assert isinstance(self.resolver_class, type)
            self.on_execute_func = None
            self.name = None

        self.kwargs = kwargs.copy()
        self.schema = schema
        self.on_get_func = None
        self.on_set_func = None
        self.on_del_func = None

    def __call__(self, func=None):
        # logic to perform lazily if this decorator was created
        # like @resolver() instead of @resolver:
        if func is not None:
            self.on_execute_func = func
            self.name = func.__name__

        return self

    def on_get(self, func):
        self.on_get_func = func
        return self

    def on_set(self, func):
        self.on_set_func = func
        return self

    def on_del(self, func):
        self.on_del_func = func
        return self


class resolver(ResolverDecorator):
    pass
