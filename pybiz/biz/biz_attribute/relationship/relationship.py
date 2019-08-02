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
from appyratus.schema.fields import Field

from pybiz.exc import RelationshipArgumentError, RelationshipError
from pybiz.util.misc_functions import (
    normalize_to_tuple,
    is_bizobj,
    is_sequence,
    is_bizlist,
)
from pybiz.util.loggers import console

from ..biz_attribute import BizAttribute, BizAttributeProperty
from ...biz_thing import BizThing


class Join(object):
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
        self.target_biz_type = self.target_fprop.biz_type
        self.source_biz_type = self.source_fprop.biz_type

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

        results = self.target_fprop.biz_type.query(
            select=select,
            where=computed_where_predicate,
            offset=offset,
            limit=limit,
            order_by=order_by,
        )
        return results

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
        self.target_biz_type = None
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

    def __repr__(self):
        if self.target_biz_type:
            target_type_name = self.target_biz_type.__name__
            source_type_name = self.biz_type.__name__
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
            func.__globals__.update(self.api.manifest.types.biz)

            # "pybiz_is_fk" is used by Query when it decides which fields to
            # load at a baseline, in order to ensure that all relationships of
            # the BizObjects loaded through this relationship have all required
            # field data to satisfy their own Relationships' join conditions.
            mocked_retval = func(MagicMock())
            mocked_retval[0].field.meta['pybiz_is_fk'] = True
            mocked_retval[1].field.meta['pybiz_is_fk'] = True

        # determine in advance what the "target" BizObject
        # class is that this relationship queries.
        try:
            self.target_biz_type = self.joins[-1](MagicMock())[1].biz_type
        except IndexError:
            console.error(
                message='badly formed "join" argument',
                data={
                    'relationship': self.name,
                    'biz_type': self.biz_type.__name__,
                }
            )
            raise

        # by default, the relationship will load all
        # required AND all "foreign key" fields.
        if not self.select:
            self.select = {
                k: None for k, f in self.target_biz_type.schema.fields.items()
                if (f.meta.get('pybiz_is_fk', False) or f.required)
            }

        self._is_bootstrapped = True

        if self.behaviors is not None:
            for behavior in self.behaviors:
                behavior.on_post_bootstrap(self)

    def execute(
        self,
        source,
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
        # override default query parameters if provided as arguments here
        select = select if select is not None else self.select
        limit = limit if limit is not None else self.limit
        offset = offset if offset is not None else self.offset

        if not order_by and self.order_by:
            order_by = [
                func(source) for func in self.order_by
            ]
        else:
            order_by = None

        # Apply the "query_simple" method when this relationship is being loaded
        # on a single BizObject; otherwise, apply "query_batch" if it is being
        # loaded by a BizList (i.e. batch load)
        perform_query = (
            self._query_simple if is_bizobj(source)
            else self._query_batch
        )
        return perform_query(
            source,
            select=select,
            where=where,
            order_by=order_by,
            offset=offset,
            limit=limit,
        )

    def _query_simple(
        self,
        root: 'BizObject',
        select: Set = None,
        where: List = None,
        limit: int = None,
        offset: int = None,
        order_by: Tuple['OrderBy'] = None,
    ) -> 'BizObject':
        """
        Recursively load this relationship on a single BizObject caller.
        """
        source = root
        target = None

        for func in self.joins:
            join = Join(source, *func(source))
            if func is self.joins[-1]:
                # only pass in the query params to the last join in the join
                # sequence, as this is the one that truly applies to the
                # "target" BizObject type being queried.
                target = join.query(
                    select=select,
                    where=where,
                    limit=limit,
                    offset=offset,
                    order_by=order_by
                )
            else:
                target = join.query(select=select)

            source = target

        # return the related BizObject or BizList we just loaded
        if not self.many:
            return target[0] if target else None
        else:
            return target

    def _query_batch(
        self,
        sources: 'BizList',
        select: Set = None,
        where: Set = None,
        order_by: Tuple['OrderBy'] = None,
        limit: int = None,
        offset: int = None,
     ) -> 'BizList':
        """
        Recursively load this relationship on a BizList caller.
        """
        if is_sequence(sources):
            sources = self.biz_type.BizList(sources)

        original_sources = sources

        # `tree` is used as a tree of BizObjects mapped to arrays of loaded
        # child BizObjects and, in the end, is used to resolve which source
        # BizObjects we zip up with which target BizObjects or BizLists.
        tree = defaultdict(list)

        # `join_objs` just keeps in memory each Join object
        # instantiated in the process of performing the querying process
        join_objs = []

        for func in self.joins:
            # the "join.query" here is configured to issue a query that loads
            # all data required by all source BizObjects' relationships, not
            # just one BizObject's relationship at a time.
            join = Join(sources, *func(sources))
            join_objs.append(join)

            # compute `targets` - the collection of ALL BizObjects related to
            # the source objects. Below, we perform logic to determine which
            # source object to zip up with which target BizObject(s)
            if func is self.joins[-1]:
                targets = join.query(
                    select=select,
                    where=where,
                    order_by=order_by,
                    limit=limit,
                    offset=offset,
                )
            else:
                targets = join.query()

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
            sources = join.target_biz_type.BizList(distinct_targets)

        # Now we compute `results`, which is a list of either BizObjects or
        # BizLists (for a many=True relationship). The caller of query() now
        # must zip up the source and result objects.
        results = []
        terminal_biz_type = join_objs[-1].target_biz_type
        for source in original_sources:
            resolved_targets = self._get_terminal_nodes(
                tree, source, terminal_biz_type, []
            )
            if self.many:
                results.append(
                    terminal_biz_type.BizList( resolved_targets, self, source)
                )
            else:
                results.append(
                    resolved_targets[0] if resolved_targets else None
                )

        return results

    def _get_terminal_nodes(self, tree, parent, target_biz_type, acc):
        """
        Follow a path in the tree dict to determine which BizObjects were loaded
        for the given parent source BizObject.
        """
        children = tree[parent]
        if not children and isinstance(parent, target_biz_type):
            acc.append(parent)
        else:
            for bizobj in children:
                self._get_terminal_nodes(tree, bizobj, target_biz_type, acc)
        return acc


class RelationshipProperty(BizAttributeProperty):

    @property
    def relationship(self):
        return self.biz_attr

    def select(self, *targets) -> 'Query':
        rel = self.relationship
        query = pybiz.biz.Query(rel.target_biz_type, rel.name)
        return (
            query
                .select(targets)
                .order_by(rel.order_by)
                .limit(rel.limit)
                .offset(rel.offset)
            )

    def fget(self, source):
        """
        Return the memoized BizObject instance or list.
        """
        rel = self.relationship
        default = rel.target_biz_type.BizList([], rel, source) if rel.many else None
        fields_to_load = set(rel.target_biz_type.schema.fields.keys())
        value = super().fget(source, select=fields_to_load) or default
        for cb_func in rel.on_get:
            cb_func(source, value)
        return value

    def fset(self, source, target):
        """
        Set the memoized BizObject or list, enuring that a list can't be
        assigned to a Relationship with many == False and vice versa.
        """
        rel = self.relationship

        if source[rel.name] and rel.readonly:
            raise RelationshipError(f'{rel} is read-only')

        if target is None and rel.many:
            target = rel.target_biz_type.BizList([], rel, target)
        elif is_sequence(target):
            target = rel.target_biz_type.BizList(target, rel, target)

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
