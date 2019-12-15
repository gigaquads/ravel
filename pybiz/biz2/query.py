from random import randint
from typing import Dict, Type, Set
from collections import OrderedDict, defaultdict

from appyratus.enum import EnumValueStr

from pybiz.util.misc_functions import is_sequence
from pybiz.constants import ID_FIELD_NAME

from .util import is_biz_object, is_biz_list
from .resolver import (
    Resolver,
    ResolverProperty,
    FieldResolver,
)


class Backfill(EnumValueStr):
    @staticmethod
    def values():
        return {'persistent', 'ephemeral'}


class QueryParameterAssignment(object):
    def __init__(self, param_name, query):
        self._query = query
        self._param_name = param_name

    def __call__(self, param):
        self._query.params[self._param_name] = param
        return self._query


class AbstractQuery(object):
    def __init__(self, parent: 'AbstractQuery' = None, *args, **kwargs):
        self._params = {}
        self._parent = parent

    def __getattr__(self, param_name):
        return QueryParameterAssignment(param_name, self)

    @property
    def params(self) -> Dict:
        return self._params

    @property
    def parent(self) -> 'AbstractQuery':
        return self._parent


class QueryBackfiller(object):
    def __init__(self):
        self._biz_class_2_objects = defaultdict(list)

    def register(self, obj):
        if is_biz_object(obj):
            self._biz_class_2_objects[type(obj)].append(obj)
        elif is_biz_list(obj):
            self._biz_class_2_objects[obj.pybiz.biz_class].extend(obj)
        elif isinstance(value, (list, tuple, set)):
            for item in obj:
                self.register(item)
        elif isinstance(dict):
            for val in obj.values():
                self.register(val)
        else:
            raise Exception('unknown argument type')

    def create_all(self):
        for biz_class, biz_objects in self._biz_class_2_objects.items():
            created = biz_class.create_many(biz_objects)


class Query(AbstractQuery):
    def __init__(self, biz_class: Type['BizObject'], *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._biz_class = biz_class
        self.clear()

    @property
    def biz_class(self):
        return self._biz_class

    def clear(self):
        self.params['select'] = OrderedDict({
            ID_FIELD_NAME: ResolverQuery(
                self._biz_class.pybiz.resolvers[ID_FIELD_NAME]
            )
        })

    def execute(self, first=False, backfill: Backfill = None):
        backfiller = None
        if backfill is not None:
            if backfill is not True:
                Backfill.validate(backfill)
            backfiller = QueryBackfiller()

        predicate = self.params.get('where')
        if not predicate:
            predicate = (self.biz_class._id != None)

        select_param = self.params.get('select')
        if not select_param:
            field_names = set(self.biz_class.pybiz.schema.fields.keys())
            resolver_queries = []
        else:
            field_names = set()
            resolver_queries = []
            for k, v in select_param.items():
                if k in self.biz_class.pybiz.schema.fields:
                    field_names.add(k)
                else:
                    resolver_queries.append(v)

        dao = self.biz_class.get_dao()

        records = dao.query(
            predicate=predicate,
            fields=field_names,
            limit=self.params.get('limit'),
            offset=self.params.get('offset'),
        )

        biz_objects = self._biz_class.BizList(
            self.biz_class(data=record).clean() for record in records
        )

        if backfiller is not None:
            if len(biz_objects) < self.params.get('limit', 1):
                backfill_count = self.params.get('limit', 1) - len(biz_objects)
                generated_biz_objects = self.generate(count=backfill_count)
                backfiller.register(generated_biz_objects)
                biz_objects.extend(generated_biz_objects)

        for biz_obj in biz_objects:
            for rq in resolver_queries:
                rq.execute(biz_obj, backfiller=backfiller)

        if backfill == Backfill.persistent:
            assert backfiller is not None
            backfiller.create_all()

        if first:
            return biz_objects[0] if biz_objects else None
        else:
            return biz_objects

    def select(self, *selectors, append=True):
        if not append:
            self.clear()

        if not selectors:
            self.params['select'].update({
                k: self._new_resolver_query(v)
                for k, v in self._biz_class.resolvers.fields.items()
            })
        else:
            flattened_selectors = []
            for x in selectors:
                if is_sequence(x):
                    flattened_selectors.extend(x)
                else:
                    flattened_selectors.append(x)

            resolver_queries = {}
            resolvers = []

            for x in flattened_selectors:
                rq = None
                resolver = None
                if isinstance(x, str):
                    resolver = self._biz_class.resolvers[x]
                    if resolver:
                        rq = self._new_resolver_query(resolver)
                    else:
                        continue
                elif isinstance(x, ResolverProperty):
                    resolver = x.resolver
                    rq = self._new_resolver_query(x.resolver)
                elif isinstance(x, ResolverQuery):
                    resolver = x.resolver
                    rq = x
                    rq.parent = self

                if resolver and rq:
                    resolvers.append(resolver)
                    resolver_queries[resolver.name] = rq

            for resolver in Resolver.sort(resolvers):
                rq = resolver_queries[resolver.name]
                self.params['select'][resolver.name] = rq

        return self

    def generate(self, count=None, first=False):
        """
        This method is really just syntactic surgar for doing query.generate()
        instead of BizObject.generate(query).
        """
        if count is None:
            count = self.params.get('limit', randint(1, 10))

        biz_objects = self.biz_class.BizList(
            self.biz_class.generate(query=self) for i in range(count)
        )
        if first:
            return biz_objects[0]
        else:
            return biz_objects

    def _new_resolver_query(self, resolver):
        return ResolverQuery(resolver, parent=self)


class ResolverQuery(Query):
    def __init__(
        self,
        resolver: 'Resolver',
        parent: 'AbstractQuery' = None,
        *args, **kwargs
    ):
        super().__init__(
            biz_class=resolver.biz_class, parent=parent, *args, **kwargs
        )
        self._resolver = resolver

    @property
    def resolver(self) -> 'Resolver':
        return self._resolver

    def clear(self):
        self.params['select'] = OrderedDict()

    def execute(self, instance: 'BizObject', backfiller=None):
        value = self._resolver.execute(instance, **self.params)
        if (not value) and (backfiller is not None):
            value = self._resolver.generate(
                instance, query=self, backfiller=backfiller
            )
            backfiller.register(value)
        setattr(instance, self._resolver.name, value)
