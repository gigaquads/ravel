import sys
import re

from copy import deepcopy
from typing import Text, Set, Dict, List, Callable, Type, Tuple

import pybiz.biz.query.query as query_module

from pybiz.constants import EMPTY_FUNCTION
from pybiz.util.loggers import console
from pybiz.util.misc_functions import get_class_name

from ..biz_thing import BizThing
from ..util import is_biz_object, is_biz_list
from .resolver_decorator import ResolverDecorator



class Resolver(object):

    @classmethod
    def decorator(cls):
        """
        Class factory method for convenience when creating a new
        ResolverDecorator for Resolvers.
        """
        class decorator_class(ResolverDecorator):
            def __init__(self, *args, **kwargs):
                super().__init__(resolver=cls, *args, **kwargs)

        class_name = f'{get_class_name(cls)}ResolverDecorator'
        decorator_class.__name__ = class_name
        return decorator_class

    def __init__(
        self,
        biz_class: Type['BizObject'] = None,
        target_biz_class: Type['BizObject'] = None,
        name: Text = None,
        many: bool = False,
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
        self._target_biz_class = None
        self._is_bootstrapped = False
        self._is_bound = False
        self._many = many
        self._target_biz_class_callback = None

        # if `target_biz_class` was not provided as a callback but as a class object, we
        # can eagerly set self._target_biz_class. otherwise, we can only call the callback
        # lazily, during the bind lifecycle method, after its lexical scope has
        # been updated with references to the BizObject types picked up by the
        # host Application.
        if not isinstance(target_biz_class, type):
            self._target_biz_class_callback = target_biz_class
        else:
            self.target_biz_class = target_biz_class or biz_class

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

        target_biz_class = ''
        if self._target_biz_class is not None:
            target_biz_class += get_class_name(self._target_biz_class)

        return (
            f'Resolver('
            f'name={source}, '
            f'target_biz_class={target_biz_class}, '
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
        if self._target_biz_class_callback:
            biz_class.pybiz.app.inject(self._target_biz_class_callback)
            self.target_biz_class = self._target_biz_class_callback()

        self.on_bind(biz_class)
        self._is_bound = True

    def on_bind(self, biz_class):
        pass

    @property
    def target_biz_class(self) -> Type['BizObject']:
        return self._target_biz_class

    @target_biz_class.setter
    def target_biz_class(self, target_biz_class: Type['BizThing']):
        if (
            (not isinstance(target_biz_class, type)) and
            callable(target_biz_class)
        ):
            target_biz_class = target_biz_class()

        if target_biz_class is None:
            # default value
            self._target_biz_class = self._biz_class
        elif is_biz_object(target_biz_class):
            self._target_biz_class = target_biz_class
            self._many = False
        elif is_biz_list(target_biz_class):
            self._target_biz_class = target_biz_class.pybiz.biz_class
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
        new_query = query_module.ResolverQuery(
            resolver=self, biz_class=self._target_biz_class
        )

        if parent_query is not None:
            new_query.configure(parent_query.options)
            if parent_query.root is not None:
                new_query.root = parent_query.root
            else:
                new_query.root = parent_query

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
        elif self._target_biz_class is not None:
            assert isinstance(value, BizThing)
            return value.dump()
        else:
            raise NotImplementedError()

    def generate(self, owner: 'BizObject', query: 'ResolverQuery'):
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


class StoredQueryResolver(Resolver):
    def __init__(self, query, *args, **kwargs):
        super().__init__(target_biz_class=query.biz_class, *args, **kwargs)
        self._query = query

    @staticmethod
    def on_execute(
        owner: 'BizObject',
        resolver: 'Resolver',
        request: 'QueryRequest'
    ):
        merged_query = self._query.merge(request.query)
        return merged_query.execute()

    @staticmethod
    def on_backfill(
        owner: 'BizObject',
        resolver: 'Resolver',
        request: 'QueryRequest',
        result
    ):
        merged_query = self._query.merge(request.query)
        return merged_query.generate()
