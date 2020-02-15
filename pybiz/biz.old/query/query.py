from random import randint
from typing import Dict, Type, Set, Text
from collections import OrderedDict, defaultdict
from copy import deepcopy

from appyratus.utils import DictObject
from appyratus.env import Environment

from pybiz.util.loggers import console
from pybiz.util.misc_functions import (
    is_sequence,
    get_class_name,
    flatten_sequence,
)
from pybiz.constants import ID_FIELD_NAME
from pybiz.schema import String
from pybiz.predicate import (
    OP_CODE,
    OP_CODE_2_DISPLAY_STRING,  # TODO: Turn OP_CODE into proper enum
    ConditionalPredicate,
    Predicate,
)

from ..resolver.resolver import Resolver, StoredQueryResolver
from ..resolver.resolver_property import ResolverProperty
from ..util import is_resource, is_batch
from .request import QueryRequest, QueryResponse
from .backfill import Backfill, QueryBackfiller
from .printer import QueryPrinter
from .order_by import OrderBy


class QueryOptions(object):

    env = Environment()

    def __init__(
        self,
        first=False,
        echo=None,
    ):
        self._data = {
            'first': first,
            'echo': (
                echo if echo is not None else
                bool(self.env.PYBIZ_ECHO_QUERY)
            )
        }

    def update(self, obj):
        if isinstance(obj, QueryOptions):
            self._data.update(obj._data)
        else:
            self._data.update(obj)

    def get(self, key, default=None):
        return self._data.get(key, default)

    def __getitem__(self, key):
        return self._data[key]

    def __setitem__(self, key, value):
        self._data[key] = value


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

    def __repr__(self):
        return (
            f'QueryParameterBinder('
            f'param={self._param_name}, '
            f'query={self._query})'
        )


# TODO:Merge AbstractQuery into Query
class AbstractQuery(object):
    """
    Abstract base class/interface for Query.
    """

    Options = QueryOptions

    def __init__(
        self,
        parent: 'AbstractQuery' = None,
        options: 'QueryOptions' = None,
        *args, **kwargs
    ):
        self._options = options or QueryOptions()

        self._parent = parent
        self._params = DictObject()
        self._params.select = OrderedDict()
        self._params.subqueries = OrderedDict()

    def __getattr__(self, param_name):
        return QueryParameterAssignment(param_name, self)

    @property
    def params(self) -> Dict:
        return self._params

    @property
    def parent(self) -> 'AbstractQuery':
        return self._parent

    @parent.setter
    def parent(self, parent) -> 'AbstractQuery':
        self._parent = parent

    @property
    def options(self) -> 'QueryOptions':
        return self._options

    def merge(self, other, in_place=False) -> 'AbstractQuery':
        if in_place:
            self._params.update(deepcopy(other._params))
            self._options.update(deepcopy(other._options))
            return self
        else:
            merged_query = type(self)()
            merged_query.merge(self, in_place=True)
            merged_query.merge(other, in_place=True)
            return merged_query

    def configure(
        self, options: 'QueryOptions' = None, **more_options
    ) -> 'AbstractQuery':
        if options is not None:
            self._options.update(options)
        if more_options:
            self._options.update(more_options)
        return self

    def select(self, *resolvers, **subqueries):
        raise NotImplementedError()

    def execute(self, first=False, backfill: Backfill = None):
        raise NotImplementedError()

    def generate(self, count=None, first=False):
        raise NotImplementedError()


class Query(AbstractQuery):

    printer = QueryPrinter()

    def __init__(self, biz_class: Type['Resource'], *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._biz_class = biz_class
        self._response = None
        self.clear()

    def __repr__(self):
        biz_class_name = ''
        if self._biz_class is not None:
            biz_class_name = get_class_name(self._biz_class)
        return (
            f'Query(target={biz_class_name})'
        )

    @property
    def biz_class(self):
        return self._biz_class

    @property
    def response(self):
        return self._response

    @property
    def batch_class(self):
        return self._biz_class.Batch if self._biz_class else None

    def clear(self, select=True, where=True):
        if select:
            self.params.select = OrderedDict()
            self.params.subqueries = OrderedDict()
        if where:
            self.params.where = None

        return self

    def printf(self):
        self.printer.printf(self)

    def fprintf(self):
        return self.printer.fprintf(self)

    def bind(self, sources: Dict[Text, 'Entity'] = None, **more_sources):
        sources = sources or {}
        sources.update(more_sources)

        if self.params.where:
            self.params.where.bind(sources)

        return self

    def where(self, *predicates, **equality_values):
        computed_predicate = Predicate.reduce_and(predicates)
        if equality_values:
            equality_predicates = [
                ConditionalPredicate(
                    OP_CODE.EQ, getattr(self._biz_class, key), value
                )
                for key, value in equality_values.items()
            ]
            computed_predicate = Predicate.reduce_and(
                computed_predicate, equality_predicates
            )

        if self.params.where is not None:
            self.params.where &= computed_predicate
        else:
            self.params.where = computed_predicate

        return self

    def order_by(self, *objects):
        objects = flatten_sequence(objects)
        if objects:
            self.params.order_by = []
        for obj in objects:
            if isinstance(obj, OrderBy):
                self.params.order_by.append(obj)
            elif isinstance(obj, str):
                if obj.lower().endswith(' desc'):
                    order_by_obj = OrderBy(obj.split()[0], desc=True)
                else:
                    order_by_obj = OrderBy(obj.split()[0], desc=False)
                self.params.order_by.append(order_by_obj)
        return self

    def execute(
        self,
        request: 'QueryRequest' = None,
        context: Dict = None,
        backfill: Backfill = None,
        first: bool = None,
        response: bool = False,
    ):
        """
        Execute the Query. This is not a recursive procedure in and of itself.
        Any selected Resolver may or may not create and run its own Query within
        its on_execute logic.
        """
        if first is not None:
            self._options['first'] = first

        # init the request, which holds a ref to the query, a Backfiller and the
        # context dict. the backfiller and context dict are passed along to each
        # resolver on execute.
        request = self._init_request(backfill, context, parent_request=request)

        self.log()

        # fetch data from the DAL, create the Resources and execute their
        # resolvers. call these objects the "targets" throughout the code.
        records = self._execute_dal_query(request)
        targets = self._instantiate_targets_and_resolve(records, request, first)

        # save all backfilled objects if "persistent" mode is toggled
        if backfill == Backfill.persistent:
            request.backfiller.save_many()

        # transform the final return value, which may
        # be a single object or multiple.
        retval = self._compute_execution_return_value(request, targets)

        self._execute_subqueries(targets, request)

        # If the `response` kwarg is set, return the response object
        # rather than the data. On the response, you can still access
        # the data via response.body.
        if response:
            return self.response

        return retval

    def log(self, message=None):
        if self.options['echo']:
            base_message = message or f'executing query {self}'
            console.info(
                message=f'{base_message}:\n{self.fprintf()}',
            )

    def _init_request(self, backfill, context, parent_request):
        backfiller = self._init_backfiller(backfill)
        return QueryRequest(
            query=self,
            backfiller=backfiller,
            context=context,
            parent=parent_request,
            root=parent_request.root if parent_request else self
        )

    def _init_backfiller(self, backfill):
        backfiller = None
        if backfill is not None:
            if backfill is not True:
                Backfill.validate(backfill)
            backfiller = QueryBackfiller()
        return backfiller

    def _compute_execution_return_value(self, request, targets):
        """
        Compute final return value.
        """
        retval = targets.clean()
        if self._options.get('first', False):
            retval = targets[0] if targets else None

        if request.root:
            resp = request.root.response
        else:
            resp = request.response

        # if this query has an alias, store it in the root query
        # response's "aliased" dict.
        if self.params.alias:
            alias = self.params.alias
        else:
            alias = str(id(self))

        resp.aliased[alias] = retval

        return retval

    def _instantiate_targets_and_resolve(self, target_records, request, first):
        targets = self.batch_class(self._biz_class(x) for x in target_records)

        if first:
            request.response = QueryResponse(
                self, targets[0] if targets else None
            )
        else:
            request.response = QueryResponse(self, targets)

        self._response = request.response
        self._execute_target_resolver_queries(targets, request)
        return targets

    def _build_request(self, query, source, parent, resolver):
        return QueryRequest(
            query=query,
            source=source,
            resolver=resolver,
            backfiller=parent.backfiller,
            context=parent.context,
            parent=parent,
        )

    def _execute_target_resolver_queries(self, targets, parent_request):
        """
        Call all ResolverQuery execute methods on all target Resources.
        """
        for resolver_name, query in self.params.select.items():
            if resolver_name in self._biz_class.pybiz.resolvers.fields:
                # TODO: manually perform pre and post-execute
                continue

            for target in targets:
                # resolve and set the value to set in the
                # target Resource's state dict
                resolver = query.resolver
                request = self._build_request(
                    query, target, parent_request, resolver
                )
                # Note below that the resolver sets the resolved
                # value on the calling Resource via resolver.execute
                query.execute(request)

    def _execute_subqueries(self, targets, request):
        for name, subquery in self.params.subqueries.items():
            for target in targets:
                value = subquery.execute(request=request)
                resolver = StoredQueryResolver(subquery, name=name)
                target.internal.resolvers.register(resolver)
                target.internal.state[name] = value

    def _execute_dal_query(self, request):
        fields = self._extract_selected_field_names()
        predicate = self._compute_where_predicate(request)
        return self._biz_class.get_store().dispatch(
            method_name='query',
            args=(predicate, ),
            kwargs=dict(
                fields=fields,
                limit=self.params.get('limit'),
                offset=self.params.get('offset'),
                order_by=self.params.get('order_by'),
            )
        )

    def _extract_selected_field_names(self) -> Set[Text]:
        resolver_queries = self.params.get('select')
        if not resolver_queries:
            store_field_names = set(self._biz_class.pybiz.schema.fields.keys())
        else:
            store_field_names = {
                k for k in resolver_queries if k in
                self._biz_class.pybiz.resolvers.fields
            }

        store_field_names.add(ID_FIELD_NAME)
        return store_field_names

    def _compute_where_predicate(self, request):
        predicate = self.params.get('where')

        if not predicate:
            # ensure we at least have a default "select *" predicate
            # to use when executing this Query in the Store
            predicate = (self._biz_class._id != None)
        elif self.parent is not None and predicate.is_unbound:
            if request.root is None:
                raise Exception('no parent data to bind')
            predicate.bind(request.root.response.aliased)

        return predicate

    def select(self, *targets, **subqueries):
        targets = flatten_sequence(targets)
        resolver_queries = {}
        resolvers = []

        for obj in targets:
            resolver_query = None
            resolver = None

            if isinstance(obj, str):
                obj = self._biz_class.pybiz.resolvers[obj]

            if isinstance(obj, Resolver):
                resolver = obj
                resolver_query = resolver.select(parent_query=self)
            elif isinstance(obj, ResolverProperty):
                resolver = obj.resolver
                resolver_query = resolver.select(parent_query=self)
            elif isinstance(obj, ResolverQuery):
                resolver = obj.resolver
                resolver_query = obj
            elif isinstance(obj, type) and is_resource(obj):
                assert obj is self._biz_class
                self.select(obj.pybiz.resolvers.values())

            if resolver and resolver_query:
                resolvers.append(resolver)
                resolver_queries[resolver.name] = resolver_query

        for resolver in Resolver.sort(resolvers):
            resolver_query = resolver_queries[resolver.name]
            self.params.select[resolver.name] = resolver_query

        for name, subquery in subqueries.items():
            subquery.parent = self
            self.params.subqueries[name] = subquery

        return self

    def generate(self, count=None):
        """
        This method is really just syntactic surgar for doing query.generate()
        instead of Resource.generate(query).
        """
        # TODO: extend to support subqueries
        if count is None:
            count = self.params.get('limit', randint(1, 10))

        resources = self._biz_class.Batch(
            self._biz_class.generate(query=self) for i in range(count)
        )
        if self._options.get('first', False):
            return resources[0]
        else:
            return resources


class ResolverQuery(Query):
    def __init__(
        self,
        resolver: 'Resolver',
        *args, **kwargs
    ):
        super().__init__(*args, **kwargs)
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
        return self

    def execute(self, request: 'QueryRequest'):
        """
        Execute Resolver.execute (and backfill its value if necessary).
        """
        # resolve the value to set in source Resource's state
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
