from typing import Dict, Type, Set
from collections import OrderedDict

from pybiz.util.misc_functions import is_sequence
from pybiz.constants import ID_FIELD_NAME

from .resolver import (
    Resolver,
    ResolverProperty,
    FieldResolver,
)


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

    def execute(self, first=False):
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
        )

        # TODO: apply FieldResolver transforms
        biz_objects = [
            cls(data=record) for record in records
        ]

        for biz_obj in biz_objects:
            for rq in resolver_queries:
                rq.execute(biz_obj)

    def select(self, *selectors, append=True):
        if not append:
            self.clear()

        if not selectors:
            self.params['select'].update({
                k: ResolverQuery(v)
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
                        rq = ResolverQuery(resolver)
                    else:
                        continue
                elif isinstance(x, ResolverProperty):
                    resolver = x.resolver
                    rq = ResolverQuery(x.resolver)
                elif isinstance(x, ResolverQuery):
                    resolver = x.resolver
                    rq = x

                if resolver and rq:
                    resolvers.append(resolver)
                    resolver_queries[resolver.name] = rq

            for resolver in Resolver.sort(resolvers):
                rq = resolver_queries[resolver.name]
                self.params['select'][resolver.name] = rq

        return self

    def generate(self, count=1, first=False):
        """
        This method is really just syntactic surgar for doing query.generate()
        instead of BizObject.generate(query).
        """
        biz_objects = self.biz_class.BizList(
            self.biz_class.generate(query=self)
            for i in range(count)
        )
        if first:
            return biz_objects[0]
        else:
            return biz_objects




class ResolverQuery(Query):
    def __init__(self, resolver, *args, **kwargs):
        super().__init__(biz_class=resolver.biz_class, *args, **kwargs)
        self._resolver = resolver

    @property
    def resolver(self):
        return self._resolver

    def clear(self):
        self.params['select'] = OrderedDict()

    def execute(self, instance: 'BizObject'):
        value = self._resolver.execute(instance, **self.params)
        setattr(instance, self._resolver.name, value)
