from random import randint
from typing import Dict, Type, Set, Text
from collections import OrderedDict, defaultdict
from copy import deepcopy
from types import GeneratorType

from appyratus.utils import DictObject

from pybiz.util.misc_functions import is_sequence, get_class_name
from pybiz.constants import ID_FIELD_NAME
from pybiz.schema import String
from pybiz.predicate import (
    OP_CODE,
    OP_CODE_2_DISPLAY_STRING,  # TODO: Turn OP_CODE into proper enum
    ConditionalPredicate,
    Predicate,
)

from ..resolver.resolver import Resolver
from ..resolver.resolver_property import ResolverProperty
from ..util import is_biz_object, is_biz_list
from .request import QueryRequest
from .backfill import Backfill, QueryBackfiller
from .printer import QueryPrinter
from .order_by import OrderBy


class QueryOptions(object):
    def __init__(self, eager=True):
        self._data = {
            'eager': eager,
        }

    def to_dict(self):
        return deepcopy(self._data)

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


class AbstractQuery(object):
    """
    Abstract base class/interface for Query.
    """

    Options = QueryOptions

    def __init__(
        self,
        parent: 'AbstractQuery' = None,
        options: 'QueryOptions' = None,
        *args,
        **kwargs
    ):
        self._options = options or QueryOptions()
        self._parent = parent
        self._params = DictObject()
        self._params.select = OrderedDict()

    def __getattr__(self, param_name):
        return QueryParameterAssignment(param_name, self)

    @property
    def params(self) -> Dict:
        return self._params

    @property
    def parent(self) -> 'AbstractQuery':
        return self._parent

    @property
    def options(self) -> 'QueryOptions':
        return self._options

    def configure(
        self, options: 'QueryOptions' = None, **more_options
    ) -> 'AbstractQuery':
        if options is not None:
            self._options.update(options)
        if more_options:
            self._options.update(more_options)
        return self

    def select(self, *selectors, append=True):
        raise NotImplementedError()

    def execute(self, first=False, backfill: Backfill = None):
        raise NotImplementedError()

    def generate(self, count=None, first=False):
        raise NotImplementedError()


class Query(AbstractQuery):

    printer = QueryPrinter()

    def __init__(self, biz_class: Type['BizObject'], *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._biz_class = biz_class
        self.clear()

    def __repr__(self):
        biz_class_name = ''
        if self._biz_class is not None:
            biz_class_name = get_class_name(self._biz_class)
        offset = str(self.params.get('offset') or '')
        limit = str(self.params.get('limit') or '')
        return (
            f'Query'
            f'(target={biz_class_name}[{offset}:{limit}])>'
        )

    @property
    def biz_class(self):
        return self._biz_class

    @property
    def biz_list_class(self):
        return self._biz_class.BizList if self._biz_class else None

    def clear(self, select=True, where=True):
        if select:
            self.params.select = OrderedDict()
            self.params.select[ID_FIELD_NAME] = ResolverQuery(
                biz_class=self._biz_class,
                resolver=self._biz_class.pybiz.resolvers[ID_FIELD_NAME],
                parent=self,
            )
        if where:
            self.params.where = None

        # Include fields maked as required to be safe, as these
        # more than likely will be referenced by some Resolver's
        # execution logic.
        if self.options.get('eager', True):
            self.select(self._biz_class.pybiz.resolvers.required_resolvers)

        return self

    def printf(self):
        self.printer.printf(self)

    def fprintf(self):
        return self.printer.fprintf(self)

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

    def execute(self, request=None, context=None, first=False, backfill: Backfill = None):
        """
        Execute the Query. This is not a recursive procedure in and of itself.
        Any selected Resolver may or may not create and run its own Query within
        its on_execute logic.
        """
        # init the request, which holds a ref to the query, a Backfiller and the
        # context dict. the backfiller and context dict are passed along to each
        # resolver on execute.
        request = self._init_request(backfill, context, parent=request)

        # fetch data from the DAL, create the BizObjects and execute their
        # resolvers. call these objects the "targets" throughout the code.
        records = self._execute_dal_query()
        targets = self._instantiate_targets_and_resolve(records, request)

        # save all backfilled objects if "persistent" mode is toggled
        if backfill == Backfill.persistent:
            request.backfiller.save_many()

        # transform the final return value, which may
        # be a single object or multiple.
        retval = self._compute_exeecution_return_value(targets, first)
        return retval

    def _init_request(self, backfill, context, parent):
        backfiller = self._init_backfiller(backfill)
        return QueryRequest(
            query=self,
            backfiller=backfiller,
            context=context,
            parent=parent,
        )

    def _init_backfiller(self, backfill):
        backfiller = None
        if backfill is not None:
            if backfill is not True:
                Backfill.validate(backfill)
            backfiller = QueryBackfiller()
        return backfiller

    def _compute_exeecution_return_value(self, targets, first):
        """
        Compute final return value.
        """
        retval = targets.clean()
        if first:
            retval = targets[0] if targets else None
        return retval

    def _instantiate_targets_and_resolve(self, target_records, request):
        targets = self.biz_list_class(self.biz_class(x) for x in target_records)
        self._execute_target_resolver_queries(targets, request)
        return targets

    def _execute_target_resolver_queries(self, targets, parent_request):
        """
        Call all ResolverQuery execute methods on all target BizObjects.
        """
        def build_request(query, source, parent, resolver):
            return QueryRequest(
                query=query,
                source=source,
                resolver=resolver,
                backfiller=parent.backfiller,
                context=parent.context,
                parent=parent,
            )

        for query in self.params.select.values():
            for target in targets:
                # resolve and set the value to set in the
                # target BizObject's state dict
                resolver = query.resolver
                request = build_request(
                    query, target, parent_request, resolver
                )
                # Note below that the resolver sets the resolved
                # value on the calling BizObject via resolver.execute
                query.execute(request)

    def _execute_dal_query(self):
        fields = self._extract_selected_field_names()
        predicate = self._compute_where_predicate()
        return self.biz_class.get_dao().query(
            predicate=predicate,
            fields=fields,
            limit=self.params.get('limit'),
            offset=self.params.get('offset'),
            order_by=self.params.get('order_by'),
        )

    def _extract_selected_field_names(self) -> Set[Text]:
        resolver_queries = self.params.get('select')
        if not resolver_queries:
            dao_field_names = set(self.biz_class.pybiz.schema.fields.keys())
        else:
            dao_field_names = {
                k for k in resolver_queries if k in
                self.biz_class.pybiz.resolvers.fields
            }

        return dao_field_names

    def _compute_where_predicate(self):
        # ensure we at least have a default "select *" predicate
        # to use when executing this Query in the Dao
        predicate = self.params.get('where')
        if not predicate:
            predicate = (self.biz_class._id != None)
        return predicate

    def select(self, *selectors, append=True):
        if not append:
            self.clear()

        flattened_selectors = []
        for obj in selectors:
            if is_sequence(obj):
                flattened_selectors.extend(obj)
            if isinstance(obj, GeneratorType):
                flattened_selectors.extend(list(obj))
            else:
                flattened_selectors.append(obj)

        resolver_queries = {}
        resolvers = []

        for obj in flattened_selectors:
            resolver_query = None
            resolver = None

            if isinstance(obj, str):
                obj = self._biz_class.pybiz.resolvers[obj]

            if isinstance(obj, Resolver):
                resolver = obj
                resolver_query = resolver.select()
                resolver_query.configure(self.options)
            elif isinstance(obj, ResolverProperty):
                resolver = obj.resolver
                resolver_query = resolver.select()
                resolver_query.configure(self.options)
            elif isinstance(obj, ResolverQuery):
                resolver = obj.resolver
                resolver_query = obj

            if resolver and resolver_query:
                resolvers.append(resolver)
                resolver_queries[resolver.name] = resolver_query

        for resolver in Resolver.sort(resolvers):
            resolver_query = resolver_queries[resolver.name]
            self.params.select[resolver.name] = resolver_query

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
