from typing import Text, Tuple, List, Set, Dict, Type, Union, Callable
from collections import defaultdict

from pybiz.util.loggers import console
from pybiz.util.misc_functions import (
    get_class_name,
    flatten_sequence,
)
from pybiz.predicate import (
    Predicate,
    ConditionalPredicate,
    BooleanPredicate,
    ResolverAlias,
    OP_CODE,
)


from .util import is_resource, is_batch
from .order_by import OrderBy


class Resolver(object):

    app = None

    def __init__(
        self,
        name=None,
        owner=None,
        target=None,
        on_resolve=None,
        private=False,
        required=False,
    ):
        self.name = name
        self.on_resolve = on_resolve or self.on_resolve
        self.owner = owner
        self.private = private
        self.required = required
        self.target_callback = None
        self.target = None
        self.many = None

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

    def resolve(self, resource, request):
        self.pre_resolve(resource, request)
        result = self.on_resolve(resource, request)
        processed_result = self.post_resolve(resource, request, result)
        return processed_result

    def on_bind(self):
        pass

    @classmethod
    def on_bootstrap(cls):
        pass

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

    @staticmethod
    def pre_resolve(resource, request):
        return

    @staticmethod
    def on_resolve(resource, request):
        raise NotImplementedError()

    @staticmethod
    def post_resolve(resource, request, result):
        return result


class ResolverProperty(property):
    def __init__(self, resolver):
        self.resolver = resolver
        super().__init__(
            fget=self.fget,
            fset=self.fset,
            fdel=self.fdel,
        )

    def select(self, *selectors):
        selectors = flatten_sequence(selectors)
        return Request(self.resolver, selectors=selectors)

    def fget(self, owner):
        resolver = self.resolver
        if resolver.name not in owner.internal.state:
            request = Request(resolver)
            owner.internal.state[resolver.name] = resolver.resolve(owner, request)
        return owner.internal.state.get(resolver.name)

    def fset(self, owner, value):
        resolver = self.resolver
        owner.internal.state[resolver.name] = value

    def fdel(self, owner):
        resolver = self.resolver
        owner.internal.state.pop(resolver.name)


class ResolverDecorator(object):
    def __init__(
            self, target=None, resolver=None, on_resolve=None,
            *args, **kwargs
        ):
        self.resolver_type = resolver or Resolver
        self.target = target
        self.on_resolve = on_resolve
        self.args = args
        self.kwargs = kwargs

    def __call__(self, on_resolve):
        self.on_resolve = on_resolve
        return self

    def build_resolver_property(self, owner, name):
        resolver_property = self.resolver_type.build_property(
            name=name,
            owner=owner,
            target=self.target,
            on_resolve=self.on_resolve,
            *self.args,
            **self.kwargs
        )
        return resolver_property


class EagerStoreLoader(Resolver):
    def __init__(self, field, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.field = field

    @classmethod
    def property_type(cls):
        return EagerStoreLoaderProperty

    @classmethod
    def tags(cls) -> Set[Text]:
        return {'fields'}

    @classmethod
    def priority(cls) -> int:
        return 1

    @staticmethod
    def on_resolve(resource, request):
        exists_resource = resource._id is not None
        if not exists_resource:
            return None

        unloaded_field_names = list(
            resource.Schema.fields.keys() - resource.internal.state.keys()
        )
        state = resource.store.dispatch('fetch', kwargs={
            'fields': unloaded_field_names
        })
        if state is not None:
            resource.merge(state)

        return state[request.resolver.field.name]


class EagerStoreLoaderProperty(ResolverProperty):
    def __hash__(self):
        return super().__hash__()

    def __repr__(self):
        return f'{get_class_name(self.resolver.owner)}.{self.resolver.name}'

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
        others = {obj._id if is_resource(obj) else obj for obj in others}
        return ConditionalPredicate(
            OP_CODE.INCLUDING, self, others, is_scalar=False
        )

    def excluding(self, *others) -> Predicate:
        others = flatten_sequence(others)
        others = {obj._id if is_resource(obj) else obj for obj in others}
        return ConditionalPredicate(
            OP_CODE.EXCLUDING, self, others, is_scalar=False
        )

    def fset(self, owner: 'Resource', value):
        field = self.resolver.field
        if value is None and field.nullable:
            processed_value = None
        else:
            processed_value, errors = field.process(value)
            if errors:
                raise Exception('ValidationError: ' + str(errors))
        super().fset(owner, processed_value)

    @property
    def asc(self):
        return OrderBy(self.resolver.field.name, desc=False)

    @property
    def desc(self):
        return OrderBy(self.resolver.field.name, desc=True)


class Relationship(Resolver):

    class Join(object):
        def __init__(self, left, right):
            self.left = left
            self.right = right

        def build_query(self, source):
            query = self.right.resolver.owner.select()

            if is_resource(source):
                source_value = getattr(source, self.left.resolver.field.name)
                query.where(self.right == source_value)
            else:
                assert is_batch(source)
                source_values = getattr(source, self.left.resolver.field.name)
                query.where(self.right.including(source_values))

            return query

    def __init__(self, join, *args, **kwargs):
        if callable(join):
            self.join_callback = join
            self.joins = []
        else:
            self.join_callback = None
            self.joins = join

        super().__init__(*args, **kwargs)

    @classmethod
    def tags(cls) -> Set[Text]:
        return {'relationships'}

    @classmethod
    def priority(cls) -> int:
        return 10

    def on_bind(self):
        if self.join_callback is not None:
            self.app.inject(self.join_callback)
        self.joins = [self.Join(l, r) for l, r in self.join_callback()]
        self.target = self.joins[-1].right.resolver.owner

    @staticmethod
    def pre_resolve(resource, request):
        # TODO: build, execute query, set on request.result
        rel = request.resolver
        source = resource
        joins = rel.joins
        final_join = joins[-1]

        results = []

        if len(joins) == 1:
            query = final_join.build_query(source)
            query.select(final_join.right.resolver.owner.pybiz.resolvers.fields)
            result = query.execute(first=not rel.many)
            results.append(result)
        else:
            for j1, j2 in zip(joins, join[1:]):
                query = j1.build_query(source)
                query.select(j2.left.resolver.field.name)
                if j2 is final_join:
                    results.append(query.execute(first=not rel.many))
                else:
                    results.append(query.execute())

        request.result = results[-1]


class ResolverManager(object):
    @classmethod
    def copy(cls, manager):
        copy = cls()
        copy._resolvers = manager._resolvers.copy()
        copy._tag_2_resolvers = manager._tag_2_resolvers.copy()
        copy._required_resolvers = manager._required_resolvers.copy()
        copy._private_resolvers = manager._private_resolvers.copy()
        return copy

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


class relationship(ResolverDecorator):
    def __init__(self, *args, **kwargs):
        super().__init__(resolver=Relationship, *args, **kwargs)
