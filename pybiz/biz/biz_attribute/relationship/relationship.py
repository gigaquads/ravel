import inspect

import pybiz.biz.biz_list
import pybiz.biz.query

from copy import copy
from functools import reduce
from typing import Text, Type, Tuple, Dict, Set, Callable, List
from inspect import Parameter
from collections import defaultdict

from mock import MagicMock
from appyratus.memoize import memoized_property
from appyratus.schema import ConstantValueConstraint, RangeConstraint

from pybiz.schema import Field, fields
from pybiz.exceptions import RelationshipArgumentError, RelationshipError
from pybiz.predicate import TYPE_BOOLEAN, TYPE_CONDITIONAL, OP_CODE
from pybiz.util.misc_functions import (
    normalize_to_tuple,
    is_bizobj,
    is_sequence,
    is_bizlist,
)
from pybiz.util.loggers import console

from ..biz_attribute import BizAttribute, BizAttributeProperty
from ...biz_thing import BizThing


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
        self.target_biz_class = None
        self.readonly = readonly

        # Default relationship-level query params:
        self.select = select if select else set()
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
        self._is_first_execution = True
        self._order_by_keys = set()
        self._foreign_keys = set()

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
    def is_bootstrapped(self):
        return self._is_bootstrapped

    def on_bootstrap(self):
        if self._is_bootstrapped:
            return

        if self.behaviors is not None:
            for behavior in self.behaviors:
                behavior.on_pre_bootstrap(self)

        for func in self.joins:
            # add all BizObject classes to the lexical scope of
            # each callable to prevent import errors/cycles
            func.__globals__.update(self.app.manifest.types.biz)

            # "pybiz_is_fk" is used by Query when it decides which fields to
            # load at a baseline, in order to ensure that all relationships of
            # the BizObjects loaded through this relationship have all required
            # field data to satisfy their own Relationships' join conditions.
            join_info = func(MagicMock())
            self._foreign_keys.add(join_info[0].field.name)

        # determine in advance what the "target" BizObject
        # class is that this relationship queries.
        try:
            self.target_biz_class = self.joins[-1](MagicMock())[1].biz_class
        except IndexError:
            console.error(
                message='badly formed "join" argument',
                data={
                    'relationship': self.name,
                    'biz_class': self.biz_class.__name__,
                }
            )
            raise

        for func in self.order_by:
            spec = func(MagicMock())
            field = self.target_biz_class.schema.fields[spec.key]
            self._order_by_keys.add(field.name)

        # by default, the relationship will load all
        # required AND all "foreign key" fields.
        if not self.select:
            self.select = {
                k: None for k, f
                in self.target_biz_class.schema.fields.items()
                if (
                    f.required
                    or f.name in self._foreign_keys
                    or f.name in self._order_by_keys
                   )
            }

        # Now we update the global set of "base" selectors declared on the
        # target BizObject type. This is done in order to ensure that all fields
        # referenced by a relationship are eagerly loaded whenever this type of
        # object is queried.
        self.target_biz_class.base_selectors.update(self.select)

        self._is_bootstrapped = True

        if self.behaviors is not None:
            for behavior in self.behaviors:
                behavior.on_post_bootstrap(self)

    def generate(
        self,
        source: 'BizThing',
        select: Set = None,
        where: Set = None,
        order_by: Tuple = None,
        offset: int = None,
        limit: int = None,
    ):
        root = source
        target = None
        query_params = self._prepare_query_params(
            source, select, where, order_by, offset, limit
        )

        for func in self.joins:
            join_params = func(source)
            join = RelationshipJoinExecutor(source, *join_params)

            # the parameters set on the Relationship ctor, merged or overrident
            # with those passed in as kwargs here, are only passed into the
            # final "join" query to execute, which is the one that resolves to
            # the final target BizObject type that defines the Relationship.
            if func is self.joins[-1]:
                query = join.query(**query_params)
                if self._is_first_execution:
                    self._on_first_execution(query)
            else:
                query = join.query()

            # The source BizObject sets the value of its "joined" field to on
            # the field of the target BizObjects returned from the following
            # recursive query.generate call.
            #
            # For example, if the Relationship looks like,
            #
            # ```python
            # Relationship(lambda user: (User.account_id, Account._id))`
            # ```
            #
            # Then the generated `Account` BizObject will inherit its `_id`
            # value from `user.account_id`.
            #
            # A `Constraint` is from appyratus.schema, where it represents a
            # certain kind of constraint placed on the return value from a given
            # Field's `generate` method. An `ConstantValueConstraint` says that
            # a constant value must be returned "equal" to the given value.
            constraints = {}
            constraints[join.target_fname] = ConstantValueConstraint(
                value=getattr(source, join.source_fname)
            )

            # Now generate the fully-formed `Query`, which indirectly recurses
            # on the selected Relationships referenced in subqueries therein.
            target = query.generate(constraints=constraints)

            # output becomes input for next iteration...
            source = target

        if self._is_first_execution:
            self._is_first_execution = False

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
        biz_thing = query_func(source, query_params)

        if self._is_first_execution:
            self._is_first_execution = False

        return biz_thing

    def _query_simple(self, root: 'BizObject', params: Dict) -> 'BizThing':
        """
        Recursively load this relationship on a single BizObject caller.
        """
        source = root
        target = None

        for func in self.joins:
            join_params = func(source)
            join = RelationshipJoinExecutor(source, *join_params)

            if func is self.joins[-1]:
                # only pass in the query params to the last join in the join
                # sequence, as this is the one that truly applies to the
                # "target" BizObject type being queried.
                query = join.query(**params)
                if self._is_first_execution:
                    self._on_first_execution(query)
            else:
                query = join.query()

            target = query.execute()  # target is a BizThing
            source = target  # output becomes input for next iteration

        # return the related BizObject or BizList we just loaded
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

        # `executors` just keeps in memory each RelationshipJoinExecutor object
        # instantiated in the process of performing the querying process
        executors = []

        for func in self.joins:
            # the "join.query" here is configured to issue a query that loads
            # all data required by all source BizObjects' relationships, not
            # just one BizObject's relationship at a time.
            join_params = func(sources)
            join = RelationshipJoinExecutor(sources, *join_params)
            executors.append(join)

            # compute `targets` - the collection of ALL BizObjects related to
            # the source objects. Below, we perform logic to determine which
            # source object to zip up with which target BizObject(s)
            if func is self.joins[-1]:
                query = join.query(**params)
                if self._is_first_execution:
                    self._on_first_execution(query)
            else:
                query = join.query()

            targets = query.execute()

            # adjust data structures that we used to determine, in the end,
            # which original_source objects are to be zipped up with which
            # subsets of target objects.
            field_value_2_targets = defaultdict(list)
            distinct_targets = set()

            for bizobj in targets:
                target_field_value = bizobj[join.target_fname]
                field_value_2_targets[target_field_value].append(bizobj)
                distinct_targets.add(bizobj)

            for bizobj in sources:
                source_field_value = bizobj[join.source_fname]
                mapped_targets = field_value_2_targets.get(source_field_value)
                if mapped_targets:
                    tree[bizobj].extend(mapped_targets)

            # Make targest the new sources for the next iteration
            sources = join.target_biz_class.BizList(distinct_targets)

        # Now we compute `results`, which is a list of either BizObjects or
        # BizLists (for a many=True relationship). The caller of query() now
        # must zip up the source and result objects.
        results = []
        terminal_biz_class = executors[-1].target_biz_class
        for source in original_sources:
            resolved_targets = self._get_terminal_nodes(
                tree, source, terminal_biz_class, [], 0
            )
            if self.many:
                results.append(
                    terminal_biz_class.BizList( resolved_targets, self, source)
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

    def _on_first_execution(self, query):
        """
        Logic to run the first time either the execute or generate method runs.
        """
        # add any fields utilized in this Relationship's computed "where"
        # Predicate to the base selectors of the target BizObject so that this
        # (and any) Relationship that loads on a source BizObject, which
        # inherits the relationship, always has any field value eagerly loaded
        # beforehand.
        for predicate in query.params.where:
            self.target_biz_class.base_selectors.update(
                f.name for f in predicate.fields
            )

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


class RelationshipJoinExecutor(object):
    def __init__(
        self,
        source: BizThing,
        source_fprop: 'FieldProperty',
        target_fprop: 'FieldProperty',
        predicate: 'Predicate' = None
    ):
        self.source = source
        self.predicate = predicate
        self.target_fprop = target_fprop
        self.source_fprop = source_fprop
        self.source_fname = source_fprop.field.name
        self.target_fname = target_fprop.field.name
        self.target_biz_class = self.target_fprop.biz_class
        self.source_biz_class = self.source_fprop.biz_class

    def query(
        self,
        select=None,
        where=None,
        limit=None,
        offset=None,
        order_by=None,
        **kwargs
    ) -> 'BizList':
        computed_where_predicate = self.where_predicate
        if where:
            computed_where_predicate &= reduce(lambda x, y: x & y, where)

        select.add(self.target_fname)

        query = self.target_fprop.biz_class.query(
            select=select,
            where=computed_where_predicate,
            offset=offset,
            limit=limit,
            order_by=order_by,
            execute=False
        )
        return query

    @property
    def where_predicate(self):
        target_field_value = getattr(self.source, self.source_fprop.field.name)
        where_predicate = None
        if is_bizobj(self.source):
            where_predicate = (self.target_fprop == target_field_value)
        elif is_bizlist(self.source):
            where_predicate = (self.target_fprop.including(target_field_value))
        else:
            raise TypeError('TODO: raise custom exception')
        if self.predicate:
            where_predicate &= self.predicate
        return where_predicate


class RelationshipProperty(BizAttributeProperty):

    @property
    def relationship(self):
        return self.biz_attr

    def select(self, *selectors) -> 'Query':
        """
        Return a Query object targeting this property's Relationship BizObject
        type, initializing the Query parameters to those set in the
        Relationship's constructor. The "where" conditions are computed during
        Relationship.execute().
        """
        rel = self.relationship
        return pybiz.biz.Query(
            biz_class=rel.target_biz_class,
            alias=rel.name,
            select=selectors,
            order_by=rel.order_by,
            limit=rel.limit,
            offset=rel.offset,
        )

    def fget(self, source):
        """
        Return the memoized BizObject instance or list.
        """
        rel = self.relationship
        selectors = set(rel.target_biz_class.schema.fields.keys())
        default = (
            rel.target_biz_class.BizList([], rel, source) if rel.many else None
        )

        # get or lazy load the BizThing
        biz_thing = super().fget(source, select=selectors)
        if not biz_thing:
            biz_thing = default

        # if the data was lazy loaded for the first time and returns a BizList,
        # we want to set the source and relationship properties on it.
        if rel.many:
            if biz_thing.source is None:
                biz_thing.source = source
            if biz_thing.relationship is None:
                biz_thing.relationship = rel

        # perform callbacks set on Relationship ctor
        for cb_func in rel.on_get:
            cb_func(source, biz_thing)

        return biz_thing

    def fset(self, source, target):
        """
        Set the memoized BizObject or list, enuring that a list can't be
        assigned to a Relationship with many == False and vice versa.
        """
        rel = self.relationship

        if source[rel.name] and rel.readonly:
            raise RelationshipError(f'{rel} is read-only')

        if target is None and rel.many:
            target = rel.target_biz_class.BizList([], rel, self)
        elif is_sequence(target):
            target = rel.target_biz_class.BizList(target, rel, self)

        is_scalar = not isinstance(target, pybiz.biz.BizList)
        expect_scalar = not rel.many

        if is_scalar and not expect_scalar:
            raise ValueError(
                'relationship "{}" must be a sequence because '
                'relationship.many is True'.format(rel.name)
            )
        elif (not is_scalar) and expect_scalar:
            raise ValueError(
                'relationship "{}" cannot be a BizObject because '
                'relationship.many is False'.format(rel.name)
            )

        super().fset(source, target)

        for cb_func in rel.on_set:
            cb_func(source, target)

    def fdel(self, source):
        """
        Remove the memoized BizObject or list. The field will appear in
        dump() results. You must assign None if you want to None to appear.
        """
        super().fdel(source)
        for cb_func in self.relationship.on_del:
            cb_func(source, target)
