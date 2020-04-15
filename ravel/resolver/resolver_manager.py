from typing import Text, Tuple, List, Set, Dict, Type, Union, Callable
from collections import defaultdict

from ravel.constants import ID
from ravel.util.loggers import console
from ravel.util.misc_functions import get_class_name
from ravel.util import is_resource, is_batch
from ravel.query.order_by import OrderBy
from ravel.query.request import Request

from .resolver import Resolver


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

    def __repr__(self):
        id_resolver = self._resolvers.get(ID)
        owner_name = None
        if id_resolver.is_bootstrapped:
            owner_name = get_class_name(id_resolver.owner)
        return (
            f'{get_class_name(self)}('
            f'count={len(self)}, '
            f'owner={owner_name}'
            f')'
        )

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
    def required_resolvers(self) -> Set['Resolver']:
        return self._required_resolvers

    @property
    def private_resolvers(self) -> Set['Resolver']:
        return self._private_resolvers

    def sort(self) -> List['Resolver']:
        return Resolver.sort(list(self._resolvers.values()))

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
