import sys
import re
import inspect

from copy import deepcopy
from typing import Text, Set, Dict, List, Callable, Type, Tuple
from collections import defaultdict, OrderedDict

import pybiz.biz

from pybiz.util.loggers import console
from pybiz.util.misc_functions import (
    is_sequence,
    flatten_sequence,
    get_class_name
)
from pybiz.predicate import (
    Predicate,
    ConditionalPredicate,
    BooleanPredicate,
    OP_CODE,
)

from .biz_thing import BizThing
from .util import is_biz_object, is_biz_list
from .query import Query


def EMPTY_FUNCTION():
    pass


class Resolver(object):

    @classmethod
    def decorator(cls):
        """
        Class factory method for convenience when creating a new
        ResolverDecorator for Resolvers.
        """
        class decorator_class(ResolverDecorator):
            def __init__(self, *args, **kwargs):
                super().__init__(cls, *args, **kwargs)

        class_name = f'{get_class_name(cls)}ResolverDecorator'
        decorator_class.__name__ = class_name
        return decorator_class

    def __init__(
        self,
        biz_class: Type['BizObject'] = None,
        target: Type['BizObject'] = None,
        name: Text = None,
        lazy: bool = True,
        private: bool = False,
        required: bool = False,
        on_select: Callable = None,
        on_execute: Callable = None,
        post_execute: Callable = None,
        on_backfill: Callable = None,
        on_get: Callable = None,
        on_set: Callable = None,
        on_del: Callable = None,
    ):
        self._name = name
        self._lazy = lazy
        self._private = private
        self._required = required
        self._biz_class = biz_class
        self._target = None
        self._is_bootstrapped = False
        self._is_bound = False
        self._many = None
        self._target_callback = None

        # if `target` was not provided as a callback but as a class object, we
        # can eagerly set self._target. otherwise, we can only call the callback
        # lazily, during the bind lifecycle method, after its lexical scope has
        # been updated with references to the BizObject types picked up by the
        # host Application.
        if not isinstance(target, type):
            self._target_callback = target
        else:
            self.target = target or biz_class

        # If on_execute function is a stub, interpret this as an indication to
        # use the on_execute method defined on this Resolver class.
        if on_execute and hasattr(on_execute, '__code__'):
            func_code = on_execute.__code__.co_code
            if func_code == EMPTY_FUNCTION.__code__.co_code:
                on_execute = None

        self.on_execute = on_execute or self.on_execute
        self.post_execute = post_execute or self.post_execute
        self.on_select = on_select or self.on_select
        self.on_backfill = on_backfill or self.on_backfill
        self.on_get = on_get or self.on_set
        self.on_set = on_set or self.on_set
        self.on_del = on_del or self.on_del

    def __repr__(self):
        source = ''
        if self._biz_class is not None:
            source += f'{get_class_name(self._biz_class)}'
            if self._name is not None:
                source += '.'
        source += self._name or ''

        target = ''
        if self._target is not None:
            target += get_class_name(self._target)

        return (
            f'Resolver('
            f'name={source}, '
            f'target={target}, '
            f'type={get_class_name(self)}, '
            f'priority={self.priority()}'
            f')'
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
        if self._target_callback:
            biz_class.pybiz.app.inject(self._target_callback)
            self.target = self._target_callback()

        self.on_bind(biz_class)
        self._is_bound = True

    def on_bind(self, biz_class):
        pass

    @property
    def target(self) -> Type['BizObject']:
        return self._target

    @target.setter
    def target(self, target: Type['BizThing']):
        if (not isinstance(target, type)) and callable(target):
            target = target()

        if target is None:
            # default value
            self._target = self._biz_class
        elif is_biz_object(target):
            self._target = target
            self._many = False
        elif is_biz_list(target):
            self._target = target.pybiz.biz_class
            self._many = True
        else:
            raise ValueError()

    @property
    def many(self) -> bool:
        return self._many

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
    def lazy(self):
        return self._lazy

    @property
    def required(self):
        return self._required

    @property
    def private(self):
        return self._private

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

    def select(self, parent_query: 'Query' = None, *selectors):
        new_query = ResolverQuery(resolver=self, biz_class=self._target)

        if parent_query is not None:
            new_query.configure(parent_query.options)
        if selectors:
            new_query.select(selectors)

        return self.on_select(self, new_query, parent_query)

    def execute(self, request: 'QueryRequest'):
        """
        Return the result of calling the on_execute callback. If self.state
        is set, then we return any state data that may exist, in which case
        no new call is made.
        """
        instance = request.source
        if self.name not in instance.internal.state:
            result = self.on_execute(request.source, self, request)
            instance.internal.state[self.name] = result
        else:
            result = instance.internal.state[self.name]

        result = self.post_execute(request.source, self, request, result)
        return result

    def backfill(self, request, result):
        return self.on_backfill(request.source, request, result)

    def dump(self, dumper: 'Dumper', value):
        if value is None:
            return None
        elif self._target is not None:
            assert isinstance(value, BizThing)
            return value.dump()
        else:
            raise NotImplementedError()

    def generate(self, instance: 'BizObject', query: 'ResolverQuery'):
        raise NotImplementedError()

    @staticmethod
    def on_select(
        resolver: 'Resolver',
        query: 'ResolverQuery',
        parent_query: 'Query'
    ) -> 'ResolverQuery':
        return query

    @staticmethod
    def on_execute(
        owner: 'BizObject',
        resolver: 'Resolver',
        request: 'QueryRequest'
    ):
        raise NotImplementedError()

    @staticmethod
    def post_execute(
        owner: 'BizObject',
        resolver: 'Resolver',
        request: 'QueryRequest',
        result
    ):
        return result

    @staticmethod
    def on_backfill(
        owner: 'BizObject',
        resolver: 'Resolver',
        request: 'QueryRequest',
        result
    ):
        raise NotImplementedError()

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
    def on_save(
        resolver: 'Resolver', owner: 'BizObject', value
    ) -> 'BizThing':
        return value if isinstance(value, BizThing) else None


class ResolverManager(object):
    def __init__(self):
        self._resolvers = {}
        self._tag_2_resolvers = defaultdict(dict)
        self._required_resolvers = set()
        self._private_resolvers = set()

    def __getattr__(self, tag):
        return self.by_tag(tag)

    def __getitem__(self, name):
        return self._resolvers.get(name)

    def __setitem__(self, name, resolver):
        assert name == resolver.name
        self[name] = resolver

    def __iter__(self):
        return iter(self._resolvers)

    def __contains__(self, obj):
        if isinstance(obj, Resolver):
            return obj.name in self._resolvers
        else:
            return obj in self._resolvers

    def __len__(self):
        return len(self._resolvers)

    def get(self, key, default=None):
        return self._resolvers.get(key, default)

    def keys(self):
        return list(self._resolvers.keys())

    def values(self):
        return list(self._resolvers.values())

    def items(self):
        return list(self._resolvers.items())

    @property
    def required_resolvers(self) -> Set[Resolver]:
        return self._required_resolvers

    @property
    def private_resolvers(self) -> Set[Resolver]:
        return self._private_resolvers

    def register(self, resolver):
        name = resolver.name
        old_resolver = self._resolvers.get(name)
        if old_resolver is not None:
            del self._resolvers[name]
            if old_resolver.required:
                self._required_resolvers.remove(old_resolver)
            if old_resolver.private:
                self._private_resolvers.remove(old_resolver)
            for tag in old_resolver.tags():
                del self._tag_2_resolvers[tag][name]

        self._resolvers[name] = resolver

        if resolver.required:
            self._required_resolvers.add(resolver)
        if resolver.private:
            self._private_resolvers.add(resolver)

        for tag in resolver.tags():
            self._tag_2_resolvers[tag][name] = resolver

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
            fget=self.on_get,
            fset=self.on_set,
            fdel=self.on_del,
        )

    def __hash__(self):
        return self._hash

    @property
    def biz_class(self):
        return self.resolver.biz_class if self.resolver else None

    def select(self, *targets, append=True):
        targets = flatten_sequence(targets)
        return self.resolver.select(*targets)

    def on_get(self, owner: 'BizObject'):
        from pybiz.biz2.query import QueryRequest

        # TODO: this two-step process could be
        # made more efficient somehow
        request = QueryRequest(
            query=self.select(),
            source=owner,
            resolver=self.resolver,
        )
        obj = self.resolver.execute(request)
        if self.resolver.on_get:
            self.resolver.on_get(owner, self.resolver, obj)

        return obj

    def on_set(self, owner: 'BizObject', obj):
        key = self.resolver.name
        old_obj = owner.internal.state.pop(key, None)
        owner.internal.state[key] = obj
        if self.resolver.on_set:
            self.resolver.on_set(self.resolver, old_obj, obj)

    def on_del(self, owner: 'BizObject'):
        key = self.resolver.name
        obj = owner.internal.state.pop(key, None)
        if self.resolver.on_del:
            self.resolver.on_del(self.resolver, obj)


class FieldResolver(Resolver):
    def __init__(self, field, *args, **kwargs):
        super().__init__(
            target=kwargs.get('biz_class'),
            private=field.meta.get('private', False),
            required=field.required,
            *args, **kwargs
        )
        self._field = field

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

    def on_bind(self, biz_class: Type['BizObject']):
        """
        For FieldResolvers, the target is this owner BizObject class, since the
        field value comes from it, not some other type, as with Relationships,
        for instance.
        """
        self.target = biz_class

    @staticmethod
    def on_execute(
        owner: 'BizObject',
        resolver: 'Resolver',
        request: 'QueryRequest'
    ):
        raise NotImplementedError()

        """
        Return the field value from the owner object's state dict. Lazy load the
        field if necessary.
        """
        # lazily fetch this field if not present in the owner BizObject's state
        # dict. at the same time, eagerly fetch all other non-loaded fields.
        field_name = self._field.name
        is_value_loaded = owner.is_loaded(field_name)
        if self._lazy and (not owner.is_loaded(field_name)):
            all_field_names = owner.pybiz.resolvers.fields.keys()
            loaded_field_names = instance.internal.state.keys()
            lazy_loaded_field_names = all_field_names - field_names_not_loaded
            instance.load(lazy_loaded_field_names)

        value = instance.internal.state.get(field_name)
        return value

    def dump(self, dumper: 'Dumper', value):
        """
        Run the raw value stored in the state dict through the corresponding
        Field object's process method, which validates and possibly transforms
        the data somehow, depending on how the Field was declared.
        """
        return value


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

    def on_set(self, owner: 'BizObject', value):
        if value is None and self.field.nullable:
            processed_value = None
        else:
            processed_value, errors = self.field.process(value)
            if errors:
                raise Exception('ValidationError: ' + str(errors))
        super().on_set(owner, processed_value)

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

    def __init__(self, resolver: Type[Resolver] = None, **kwargs):
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


class ResolverQuery(Query):
    def __init__(
        self,
        resolver: 'Resolver',
        parent: 'Query' = None,
        *args, **kwargs
    ):
        super().__init__(
            parent=parent, *args, **kwargs
        )
        self._resolver = resolver

    def __repr__(self):
        biz_class_name = ''
        name = ''
        if self._resolver is not None:
            biz_class_name = get_class_name(self.resolver.biz_class)
            name = self.resolver.name
        return f'Query(target={biz_class_name}.{name})'

    @property
    def resolver(self) -> 'Resolver':
        return self._resolver

    def clear(self):
        self.params.select = OrderedDict()

    def execute(self, request: 'QueryRequest'):
        """
        Execute Resolver.execute (and backfill its value if necessary).
        """
        # resolve the value to set in source BizObject's state
        value = request.resolver.execute(request)

        # if null or insufficient number of data elements are returned
        # (according to the query's limit param), optionally backfill the
        # missing values.
        if request.backfiller:
            limit = request.query.get('limit', 1)
            has_len = hasattr(value, '__len__')
            # since we don't know for sure what type of value the resolver
            # returns, we have to resort to hacky introspection here
            if (value is None) or (has_len and len(value) < limit):
                value = request.resolver.backfill(request, value)

        return value
