from typing import Text, Type, Tuple, Dict, Set, Callable, List
from collections import defaultdict

from mock import MagicMock
from appyratus.schema import ConstantValueConstraint, RangeConstraint
from appyratus.enum import EnumValueInt

from pybiz.exceptions import RelationshipError
from pybiz.util.loggers import console
from pybiz.util.misc_functions import (
    normalize_to_tuple,
    is_bizobj,
    is_sequence,
)

from .query_builders import StaticQueryBuilder, DynamicQueryBuilder
from .relationship_property import RelationshipProperty
from ..biz_attribute import BizAttribute, BizAttributeProperty
from ...biz_thing import BizThing
from ...field_property import FieldProperty
from ...query import Query


class Relationship(BizAttribute):
    def __init__(
        self,
        join: Tuple[Callable] = None,
        select: Set = None,
        order_by: Tuple['OrderBy'] = None,
        offset: int = None,
        limit: int = None,
        many: bool = False,
        private: bool = False,
        lazy: bool = True,
        readonly: bool = False,
        behavior: 'Behavior' = None,
        on_get: Tuple[Callable] = None,
        on_set: Tuple[Callable] = None,
        on_del: Tuple[Callable] = None,
        on_rem: Tuple[Callable] = None,
        on_add: Tuple[Callable] = None,
        **kwargs
    ):
        super().__init__(**kwargs)

        self.joins = normalize_to_tuple(join)
        self.behaviors = normalize_to_tuple(behavior)
        self.many = many
        self.readonly = readonly

        # Default relationship-level query params:
        self.select = set(select) if select else set()
        self.order_by = normalize_to_tuple(order_by) if order_by else tuple()
        self.offset = offset
        self.limit = limit

        # callbacks
        self.on_get = normalize_to_tuple(on_get)
        self.on_set = normalize_to_tuple(on_set)
        self.on_rem = normalize_to_tuple(on_rem)
        self.on_add = normalize_to_tuple(on_add)
        self.on_del = normalize_to_tuple(on_del)

        self._is_bootstrapped = False
        self._join_metadata = []

    def __repr__(self):
        if self.target_biz_class:
            target_type_name = self.target_biz_class.__name__
            source_type_name = self.biz_class.__name__
        else :
            target_type_name = '?'
            source_type_name = '?'

        source = f'source={source_type_name}'
        name = f'name={self.name}' if self.name else ''

        if self.many:
            target = f'target=[{target_type_name}]'
        else:
            target = f'target={target_type_name}'

        return (
            f'<Relationship('
            f'{name}, '
            f'{source}, '
            f'{target}'
            f')>'
        )

    def build_property(self):
        return RelationshipProperty(self)

    @property
    def order_key(self):
        return 1

    @property
    def category(self):
        return 'relationship'

    @property
    def target_biz_class(self):
        if self._join_metadata:
            return self._join_metadata[-1].target_biz_class
        return None

    @property
    def is_bootstrapped(self):
        return self._is_bootstrapped

    def on_bootstrap(self):
        self._apply_behaviors_pre_bootstrap()
        self._analyze_join_funcs()
        self._analyze_order_by()
        self._apply_behaviors_post_bootstrap()

    def _apply_behaviors_pre_bootstrap(self):
        if self.behaviors is not None:
            for behavior in self.behaviors:
                behavior.on_pre_bootstrap(self)

    def _apply_behaviors_post_bootstrap(self):
        if self.behaviors is not None:
            for behavior in self.behaviors:
                behavior.on_post_bootstrap(self)

    def _analyze_join_funcs(self):
        for func in self.joins:
            # add all BizObject classes to the lexical scope of each callable
            func.__globals__.update(self.app.manifest.types.biz)
            meta = JoinMetadata(func)
            self._join_metadata.append(meta)

    def _analyze_order_by(self):
        dummy = self.biz_class.generate()
        for func in self.order_by:
            spec = func(dummy)
            field = self.target_biz_class.schema.fields[spec.key]
            self.select.add(field.name)

    def generate(
        self,
        source: 'BizThing',
        select: Set = None,
        where: Set = None,
        order_by: Tuple = None,
        offset: int = None,
        limit: int = None,
        backfiller: 'Backfiller '= None,
        fetch: bool = True,
    ):
        root = source
        target = None

        for meta in self._join_metadata:
            builder = meta.new_query_builder(source)

            # the parameters set on the Relationship ctor, merged or overrident
            # with those passed in as kwargs here, are only passed into the
            # final "join" query to execute, which is the one that resolves to
            # the final target BizObject type that defines the Relationship.
            params = {}
            if meta.func is self.joins[-1]:
                params = self._prepare_query_params(
                    source, select, where, order_by, offset, limit
                )

            query = builder.build_query(**params)

            # A `Constraint` is from appyratus.schema, where it represents a
            # certain kind of constraint placed on the return value of a given
            # Field's `generate` method. An `ConstantValueConstraint` says that
            # a specific value must be returned, i.e. not randomized.
            #
            # Using this constraint, we can ensure that the two fields joined
            # between source and target BizObjects have the same value.
            #
            # For example, if the Relationship looks like,
            #
            # ```python
            # Relationship(lambda user: (User.account_id, Account._id))`
            # ```
            #
            # Then the generated `Account` BizObject will receive its `_id`
            # value from the source User's `account_id`.
            if meta.join_type == JoinType.static:
                constraints = {
                    builder.target_fname: ConstantValueConstraint(
                        value=getattr(source, builder.source_fname)
                    )
                }
            else:
                constraints = {}

            # Now generate the fully-formed `Query`, which indirectly recurses
            # on the selected Relationships referenced in subqueries therein.
            target = query.executor.execute(
                query=query,
                backfiller=backfiller,
                constraints=constraints,
                first=False,
                fetch=fetch,
            )

            # output becomes input for next iteration...
            source = target

        # return the related BizObject or BizList we just loaded
        if not self.many:
            return target[0] if target else None
        else:
            return target

    def execute(
        self,
        source: 'BizThing',
        select: Set = None,
        where: Set = None,
        order_by: Tuple = None,
        offset: int = None,
        limit: int = None,
    ):
        """
        Recursively execute this Relationship on a caller BizObject or BizList,
        loading the related BizObject(s).
        """
        # Apply the "query_simple" method when this relationship is being loaded
        # on a single BizObject; otherwise, apply "query_batch" if it is being
        # loaded by a BizList (i.e. batch load)
        query_func = (
            self._query_simple if is_bizobj(source) else self._query_batch
        )
        # sanitize and compute kwargs for the eventual Query.execute() call
        query_params = self._prepare_query_params(
            source, select, where, order_by, offset, limit
        )

        # perform the query func, which returns the resolved and loaded
        # target BizThing (A BizList or BizObject)
        return query_func(source, query_params)

    def _query_simple(self, root: 'BizObject', params: Dict) -> 'BizThing':
        """
        Recursively load this relationship on a single BizObject caller.
        """
        source = root
        target = None
        for meta in self._join_metadata:
            builder = meta.new_query_builder(source)

            # only pass in the query params to the last join in the join
            # sequence, as this is the one that truly applies to the
            # "target" BizObject type being queried.
            if meta.func is self.joins[-1]:
                query = builder.build_query(**params)
            else:
                query = builder.build_query()

            target = query.execute().clean()  # target is a BizThing
            source = target

        if not self.many:
            return target[0] if target else None
        else:
            return target

    def _query_batch(self, sources: 'BizList', params: Dict) -> 'BizList':
        """
        Recursively load this relationship on a BizList source caller.
        """
        if is_sequence(sources):
            sources = self.biz_class.BizList(sources)

        original_sources = sources

        # `tree` is used as a tree of BizObjects mapped to arrays of loaded
        # child BizObjects and, in the end, is used to resolve which source
        # BizObjects we zip up with which target BizObjects or BizLists.
        tree = defaultdict(list)

        for meta in self._join_metadata:
            if meta.join_type == JoinType.dynamic:
                # dynamic joins cannot use the batch mechanism
                distinct_targets = set()
                for source in sources:
                    builder = meta.new_query_builder(source)

                    if meta.func is self.joins[-1]:
                        query = builder.build_query(**params)
                    else:
                        query = builder.build_query()

                    targets = query.execute().clean()

                    for target in targets:
                        if target not in distinct_targets:
                            tree[source].append(target)
                            distinct_targets.add(target)

                sources = builder.target_biz_class.BizList(distinct_targets)
                continue

            # the query built here is configured to issue a query that loads
            # all data required by all source BizObjects' relationships, not
            # just one BizObject's relationship at a time.
            builder = meta.new_query_builder(sources)

            # compute `targets` - the collection of ALL BizObjects related to
            # the source objects. Below, we perform logic to determine which
            # source object to zip up with which target BizObject(s)
            if meta.func is self.joins[-1]:
                query = builder.build_query(**params)
            else:
                query = builder.build_query()

            targets = query.execute().clean()

            # adjust data structures that we used to determine, in the end,
            # which original_source objects are to be zipped up with which
            # subsets of target objects.
            field_value_2_targets = defaultdict(list)
            distinct_targets = set()

            for bizobj in targets:
                target_field_value = bizobj[builder.target_fname]
                field_value_2_targets[target_field_value].append(bizobj)
                distinct_targets.add(bizobj)

            for bizobj in sources:
                source_field_value = bizobj[builder.source_fname]
                mapped_targets = field_value_2_targets.get(source_field_value)
                if mapped_targets:
                    tree[bizobj].extend(mapped_targets)

            # Make targest the new sources for the next iteration
            sources = builder.target_biz_class.BizList(distinct_targets)

        # Now we compute `results`, which is a list of either BizObjects or
        # BizLists (for a many=True relationship). The caller of query() now
        # must zip up the source and result objects.
        results = []
        for source in original_sources:
            resolved_targets = self._get_terminal_nodes(
                tree, source, self.target_biz_class, [], 0
            )
            if self.many:
                results.append(
                    self.target_biz_class.BizList(
                        resolved_targets, self, source
                    )
                )
            else:
                results.append(
                    resolved_targets[0] if resolved_targets else None
                )

        return results

    def _get_terminal_nodes(self, tree, parent, target_biz_class, acc, depth):
        """
        Follow a path in the tree dict to determine which BizObjects were loaded
        for the given parent source BizObject.
        """
        children = tree[parent]
        if not children and depth == len(self.joins):
            acc.append(parent)
        else:
            for bizobj in children:
                self._get_terminal_nodes(
                    tree, bizobj, target_biz_class, acc, depth+1
                )
        return acc

    def _prepare_query_params(
        self,
        source: 'BizThing',
        select: Set = None,
        where: Set = None,
        order_by: Tuple = None,
        offset: int = None,
        limit: int = None,
    ):
        """
        This cleans up the keyword arguments that eventually gets passed into a
        Query object.
        """
        where = where or tuple()

        # set proper numeric bounds for limit and offset
        limit = max(limit, 1) if limit is not None else self.limit
        offset = max(offset, 0) if offset is not None else self.offset

        # merge normalize `select` to a set
        if select is None:
            select = set()
        elif not isinstance(select, set):
            select = set(select)
        if select:
            select.update(self.select)

        # compute OrderBy information and add any field referenced therein to
        # the fields being selected so that no extra lazy loading needs to occur
        # to acheive the ordering (in cases where ordering is performed by the
        # Dao in Python)
        computed_order_by = []
        order_by = order_by or self.order_by
        if order_by:
            for obj in order_by:
                if callable(obj):
                    order_by_spec = obj(source)
                else:
                    order_by_spec = obj
                computed_order_by.append(order_by_spec)
                select.add(order_by_spec.key)

        return {
            'select': select,
            'where': where,
            'order_by': computed_order_by,
            'offset': offset,
            'limit': limit,
        }


class JoinType(EnumValueInt):
    @staticmethod
    def values():
        return {
            'static': 1,
            'dynamic': 2,
        }


class JoinMetadata(object):
    def __init__(self, func: Callable):
        self.func = func
        self.target_biz_class = None
        self.join_type = None

        # this sets target_biz_class and join_type:
        self._analyze_func(func)

    def _analyze_func(self, func: Callable):
        # further process the return value of the join func
        info = func(MagicMock())
        is_dynamic_join = is_bizobj(info[0])
        if is_dynamic_join:
            self._analyze_dynamic_join(func, info)
        else:
            self._analyze_id_join(func, info)

    def _analyze_id_join(self, func: Callable, info: Tuple):
        # for an ID-based join, the first two elements of info are the
        # field properties being joined, like (User.account_id, Account._id)
        source_fprop, target_fprop = info[:2]
        target_biz_class = target_fprop.biz_class

        # regenerate info now that we know what the target biz class is
        dummy = target_biz_class.generate()
        info = func(dummy)

        # adding the source field name to the default selectors set ensures
        # that this field is alsoways returned from Queries, ensuring further
        # that no additional lazy loading of said field is needed when this
        # relationship is executed on an instance.
        Query.add_default_selectors(
            source_fprop.biz_class, source_fprop.field.name
        )

        self.target_biz_class = target_biz_class
        self.join_type = JoinType.static

    def _analyze_dynamic_join(self, func: Callable, info: Tuple):
        target_biz_class = info[0]

        # regenerate info now that we know what the target biz class is
        dummy = target_biz_class.generate()
        info = func(dummy)

        self.target_biz_class = target_biz_class
        self.join_type = JoinType.dynamic

    def new_query_builder(self, source: 'BizThing') -> 'QueryBuilder':
        params = self.func(source)
        if self.join_type == JoinType.static:
            return StaticQueryBuilder(source, *params)
        elif self.join_type == JoinType.dynamic:
            return DynamicQueryBuilder(source, *params)
        else:
            raise Exception()  # TODO: custom exception
