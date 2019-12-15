from random import randint
from typing import Dict, Type, Set, Text
from collections import OrderedDict, defaultdict

from appyratus.enum import EnumValueStr

from pybiz.util.misc_functions import is_sequence, get_class_name
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


class QueryDumper(object):
    def dump(self, query: 'Query') -> Dict:
        """
        Recursively convert a Query object into a dict, consisting only of
        Python primitives.
        """


class QueryPrinter(object):
    """
    Pretty prints a Query object, recursively.
    """

    def printf(self, query: 'Query'):
        """
        Pretty print a query object to stdout.
        """
        print(self.fprintf(query))

    def fprintf(self, query: 'Query', indent=0) -> Text:
        """
        Generate the prettified display string and return it.
        """
        substrings = []

        substrings.append(self._build_substring_select_head(query))
        if query.params.get('select'):
            substrings.extend(self._build_substring_select_body(query, indent))

        if 'where' in query.params:
            substrings.extend(self._build_substring_where(query))
        if 'order_by' in query.params:
            substrings.append(self._build_substring_order_by(query))
        if 'offset' in query.params:
            substrings.append(self._build_substring_offset(query))
        if 'limit' in query.params:
            substrings.append(self._build_substring_limit(query))

        return '\n'.join([f'{indent * " "}{s}' for s in substrings])

    def _build_substring_select_head(self, query):
        if query.params.get('select'):
            return f'FROM {get_class_name(query.biz_class)} SELECT'
        else:
            return f'FROM {get_class_name(query.biz_class)}'

    def _build_substring_where(self, query):
        substrings = []
        substrings.append('WHERE (')
        predicates = query.params['where']
        for idx, predicate in enumerate(predicates):
            if predicate.is_boolean_predicate:
                substrings.extend(
                    self._build_substring_bool_predicate(predicate)
                )
            else:
                assert predicate.is_conditional_predicate
                substrings.append(
                    self._build_substring_cond_predicate(predicate)
                )
            substrings.append(')')

        return substrings

    def _build_substring_cond_predicate(self, predicate, indent=1):
        s_op_code = OP_CODE_2_DISPLAY_STRING[predicate.op]
        s_field = predicate.field.name
        s_value = str(predicate.value)
        return f'{indent * " "}{s_field} {s_op_code} {s_value}'

    def _build_substring_bool_predicate(self, predicate, indent=1):
        s_op_code = OP_CODE_2_DISPLAY_STRING[predicate.op]
        if predicate.lhs.is_boolean_predicate:
            s_lhs = self._build_substring_bool_predicate(
                predicate.lhs, indent=indent+1
            )
        else:
            s_lhs = self._build_substring_cond_predicate(
                predicate.lhs, indent=indent+1
            )

        if predicate.rhs.is_boolean_predicate:
            s_rhs = self._build_substring_bool_predicate(
                predicate.rhs, indent=indent+1
            )
        else:
            s_rhs = self._build_substring_cond_predicate(
                predicate.rhs, indent=indent+1
            )
        return [
            f'{indent * " "}(',
            f'{indent * " "} {s_lhs} {s_op_code}',
            f'{indent * " "} {s_rhs}',
            f'{indent * " "})',
        ]

    def _build_substring_select_body(self, query, indent: int):
        substrings = []
        resolvers = query.biz_class.pybiz.resolvers
        for resolver_query in query.params.get('select', {}).values():
            resolver = resolver_query.resolver
            if resolver.name in resolvers.fields:
                substrings.append(
                    self._build_substring_selected_field(
                        resolver_query, indent
                    )
                )
            else:
                substrings.extend(
                    self._build_substring_selected_resolver(
                        resolver_query, indent
                    )
                )

        return substrings

    def _build_substring_selected_field(self, query, indent: int):
        s_name = query.resolver.name
        s_type = get_class_name(query.resolver.field)
        return f' - {s_name}: {s_type}'

    def _build_substring_selected_resolver(self, query, indent: int):
        s_name = query.resolver.name
        substrings = []
        if s_name in query.biz_class.pybiz.resolvers.relationships:
            rel = query.resolver
            s_biz_class = get_class_name(rel.target)
            if rel.many:
                substrings.append(f' - {s_name}: [{s_biz_class}] = (')
            else:
                substrings.append(f' - {s_name}: {s_biz_class} = (')
        else:
            substrings.append(f' - {s_name} = (')
        substrings.append(self.fprintf(query, indent=indent+5))
        substrings.append(f'   )')
        return substrings

    def _build_substring_order_by(self, query):
        order_by = query.params.get('order_by', [])
        if not is_sequence(order_by):
            order_by = [order_by]
        return (
            'ORDER BY (' + ', '.join(
                f'{x.key} {"DESC" if x.desc else "ASC"}'
                for x in order_by
            ) + ')'
        )

    def _build_substring_offset(self, query):
        return f'OFFSET {query.params["offset"]}'

    def _build_substring_limit(self, query):
        return f'LIMIT {query.params["limit"]}'


class QueryBackfiller(object):
    """
    Storage and logic used during the execution of a Query with backfilling
    enabled.
    """

    def __init__(self):
        self._biz_class_2_objects = defaultdict(list)

    def register(self, obj):
        """
        Recursively extract and store all BizObjects contained in the given
        `obj` argument. This method is called by the Query object that owns this
        instance and is in the process of executing.
        """
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

    def save(self):
        """
        Save all BizObject instances created during the execution of the
        backfilled query utilizing this QueryBackfiller.
        """
        for biz_class, biz_objects in self._biz_class_2_objects.items():
            biz_class.save_many(biz_objects)

    def backfill_query(self, query, existing_biz_objects):
        """
        This method is used internally by Query.execute when the Dao doesn't
        return a sufficient number of records.
        """
        num_requested = query.params.get('limit', 1)
        num_fetched = len(existing_biz_objects)
        backfill_count = num_requested - num_fetched
        generated_biz_objects = query.generate(count=backfill_count)
        self.register(generated_biz_objects)
        return generated_biz_objects

    def backfill_resolver(self, query, instance, value):
        """
        This is used internally by ResolverQuery when its Resolver returns None
        or an empty BizList, calling for the backfilling of said value.
        """
        value = query.resolver.generate(instance, query=query, backfiller=self)
        self.register(value)
        return value


class QueryParameterAssignment(object):
    """
    This is an internal data structure, used to facilitate the syntactic sugar
    that allows you to write to query.params via funcion call notation, like
    query.foo('bar') instead of query.params['foo'] = bar.

    Instances of this class just store the query whose parameter we are going to
    set and the name of the dict key or "param name". When called, it writes the
    single argument supplied in the call to the params dict of the query.
    """

    def __init__(self, param_name, query):
        self._query = query
        self._param_name = param_name

    def __call__(self, param):
        """
        Store the `param` value in the Query's parameters dict.
        """
        self._query.params[self._param_name] = param
        return self._query


class AbstractQuery(object):
    """
    Abstract base class/interface for Query.
    """

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

    def select(self, *selectors, append=True):
        raise NotImplementedError()

    def execute(self, first=False, backfill: Backfill = None):
        raise NotImplementedError()

    def generate(self, count=None, first=False):
        raise NotImplementedError()


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
        # initialize a QueryBackfiller if we need to backfill
        backfiller = None
        if backfill is not None:
            if backfill is not True:
                Backfill.validate(backfill)
            backfiller = QueryBackfiller()

        # ensure we at least have a default "select *" predicate
        # to use when executing this Query in the Dao
        predicate = self.params.get('where')
        if not predicate:
            predicate = (self.biz_class._id != None)

        # partition the "selected" objects contained in the "select" list param
        # into a list of Field names to pass into the Dao and a list of
        # ResolverQuery objects.
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

        # Fetch the selected data from the DAL and instantiate
        # the corresponding BizObjects.
        dao = self.biz_class.get_dao()
        dao_records = dao.query(
            predicate=predicate,
            fields=field_names,
            limit=self.params.get('limit'),
            offset=self.params.get('offset'),
            order_by=self.params.get('order_by'),
        )
        biz_objects = self._biz_class.BizList(
            self.biz_class(data=data).clean()
            for data in dao_records
        )

        if backfiller and len(biz_objects) < self.params.get('limit', 1):
            generated_biz_objects = backfiller.backfill_query(self, biz_objects)
            if generated_biz_objects:
                biz_objects.extend(generated_biz_objects)

        for biz_obj in biz_objects:
            for rq in resolver_queries:
                rq.execute(biz_obj, backfiller=backfiller)

        if backfill == Backfill.persistent:
            assert backfiller is not None
            backfiller.save()

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
                    rq = x.select()
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
        if backfiller and (not value):
            value = backfiller.backfill_resolver(self, instance, value)
        setattr(instance, self._resolver.name, value)
