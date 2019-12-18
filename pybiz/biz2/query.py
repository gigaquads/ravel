from random import randint
from typing import Dict, Type, Set, Text
from collections import OrderedDict, defaultdict

from appyratus.enum import EnumValueStr
from appyratus.utils import DictObject

from pybiz.util.misc_functions import is_sequence, get_class_name
from pybiz.constants import ID_FIELD_NAME

from .util import is_biz_object, is_biz_list
from .resolver import Resolver, ResolverProperty


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
                substrings.append(f' - {s_name}: [{s_biz_class}] -> (')
            else:
                substrings.append(f' - {s_name}: {s_biz_class} -> (')
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


class QueryRequest(object):
    def __init__(
        self,
        query: 'AbstractQuery',
        backfiller: 'QueryBackfiller' = None,
        context: Dict = None,
        parent: 'QueryRequest' = None,
        root: 'QueryRequest' = None,
    ):
        self.query = query
        self.backfiller = backfiller
        self.context = context
        self.parent = parent
        self.root = root

    def __repr__(self):
        biz_class_name = get_class_name(self.query.biz_class)
        return f'<Request({biz_class_name})>'

    @property
    def params(self):
        return self.query.params


class ResolverQueryRequest(QueryRequest):
    def __init__(
        self,
        query: 'AbstractQuery',
        source: 'BizObject',
        parent: 'QueryRequest' = None,
        resolver: 'Resolver' = None,
        backfiller: 'QueryBackfiller' = None,
        context: Dict = None,
    ):
        super().__init__(
            query,
            backfiller=backfiller,
            context=context,
            parent=parent,
            root=parent.root if parent else None,
        )
        self.source = source
        self.resolver = resolver

    def __repr__(self):
        biz_class_name = get_class_name(self.query.biz_class)
        if self.resolver is not None:
            return f'<Request({biz_class_name}.{self.resolver.name})>'
        return super().__repr__()


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
        self._params = DictObject()
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

    printer = QueryPrinter()

    def __init__(self, biz_class: Type['BizObject'], *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._biz_class = biz_class
        self.clear()

    @property
    def biz_class(self):
        return self._biz_class

    @property
    def biz_list_class(self):
        return self._biz_class.BizList if self._biz_class else None

    def clear(self):
        self.params['select'] = OrderedDict()
        self.params['select'][ID_FIELD_NAME] = ResolverQuery(
            biz_class=self._biz_class,
            resolver=self._biz_class.pybiz.resolvers[ID_FIELD_NAME],
            parent=self,
        )

    def printf(self):
        self.printer.printf(self)

    def fprintf(self):
        return self.printer.fprintf(self)

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
            context=context or {},
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
            return ResolverQueryRequest(
                query=query,
                source=source,
                resolver=resolver,
                backfiller=parent.backfiller,
                context=parent.context,
                parent=parent,
            )

        for query in self.params['select'].values():
            for target in targets:
                # resolve and set the value to set in the
                # target BizObject's state dict
                resolver = query.resolver
                request = build_request(query, target, parent_request, resolver)
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

        # Include fields maked as required to be safe, as these
        # more than likely will be referenced by some Resolver's
        # execution logic.
        dao_field_names.update(
            self.biz_class.Schema.required_fields.keys()
        )

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
            if isinstance(x, Resolver):
                resolver = x
                rq = resolver.select()
            elif isinstance(x, ResolverProperty):
                resolver = x.resolver
                rq = x.select()
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
            parent=parent, *args, **kwargs
        )
        self._resolver = resolver

    def __repr__(self):
        resolver = self.resolver.name if self.resolver else ''
        return f'<{get_class_name(self)}({resolver})>'

    @property
    def resolver(self) -> 'Resolver':
        return self._resolver

    def clear(self):
        self.params['select'] = OrderedDict()

    def execute(self, request: 'ResolverQueryRequest'):
        # resolve the value to set in source BizObject's state
        value = request.resolver.execute(request)

        # if null or insufficient number of data elements are returned
        # (according to the query's limit param), optionally backfill
        # the missing values.
        if request.backfiller:
            limit = request.query.get('limit', 1)
            has_len = hasattr(value, '__len__')
            if (value is None) or (has_len and len(value) < limit):
                value = request.resolver.backfill(request, value)

        return value
