import inspect

from copy import copy
from functools import reduce
from typing import Text, Type, Tuple, Dict, Set, Callable, List
from inspect import Parameter
from collections import defaultdict

from mock import MagicMock
from appyratus.memoize import memoized_property
from appyratus.schema.fields import Field

from pybiz.util import is_bizobj, is_sequence, is_bizlist, normalize_to_tuple
from pybiz.exc import RelationshipArgumentError
from pybiz.predicate import (
    Predicate,
    ConditionalPredicate,
    BooleanPredicate,
    OP_CODE,
)

from ..biz_attribute import BizAttribute
from .batch_relationship_loader import BatchRelationshipLoader

'''
company = Relationship(
    joins=(
        lambda intent: (Intent.db_id, IntentCompany.intent_db_id),
        lambda company: (IntentCompany.company_db_id, Company.db_id),
    ),
)
'''

class Join(object):
    def __init__(self, source, source_fprop, target_fprop, predicate=None):
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
        if where is not None:
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
        offset: int = None,
        limit: int = None,
        order_by: Tuple = None,
        many=False,
        private=False,
        lazy=True,
    ):
        self._private = private
        self.joins = normalize_to_tuple(join)
        self.order_by = normalize_to_tuple(order_by)
        self.select = select
        self.many = many
        self.lazy = lazy
        self.offset = offset
        self.limit = limit
        self.target_biz_type = None
        self.on_get = tuple()

    def on_bootstrap(self):
        for func in self.joins:
            func.__globals__.update(self.registry.manifest.types.biz)
            mocked_retval = func(MagicMock())
            mocked_retval[0].field.meta['pybiz_is_fk'] = True
            mocked_retval[1].field.meta['pybiz_is_fk'] = True

        self.target_biz_type = self.joins[-1](MagicMock())[1].biz_type

        if not self.select:
            self.select = {
                k: None for k, f in self.target_biz_type.schema.fields.items()
                if (f.meta.get('pybiz_is_fk', False) or f.required)
            }

    def query(self, source, select=None, where=None, offset=None, limit=None, order_by=None):
        # TODO: pass offset, limit etc down into terminal join
        if is_bizobj(source):
            load = self._load_for_bizobj
        else:
            load = self._load_for_bizlist
        return load(
            source,
            select=select,
            where=where,
            order_by=order_by,
            offset=offset,
            limit=limit,
        )

    def set_internally(self, owner: 'BizObject', related):  # TODO: rename this
        owner.related[self.name] = related

    def _load_for_bizobj(
        self,
        root: 'BizObject',
        select: Set = None,
        where: List = None,
        limit: int = None,
        offset: int = None,
        order_by: Tuple['OrderBy'] = None,
    ) -> 'BizObject':
        source = root
        target = None
        terminal_join_func = self.joins[-1]
        for func in self.joins:
            join = Join(source, *func(source))
            if func is terminal_join_func:
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
        if not self.many:
            return target[0] if target else None
        else:
            return target

    def _load_for_bizlist(
        self,
        sources: 'BizList',
        select=None,
        where: List = None,
        limit: int = None,
        offset: int = None,
        order_by: Tuple['OrderBy'] = None,
     ) -> 'BizList':
        select = select if select is not None else self.select
        limit = limit if limit is not None else self.limit
        offset = offset if offset is not None else self.offset
        order_by = order_by if order_by else self.order_by

        if is_sequence(sources):
            sources = self.biz_type.BizList(sources)

        original_sources = sources
        paths = defaultdict(list)
        evaluated_joins = []
        terminal_join_func = self.joins[-1]
        for func in self.joins:
            join = Join(sources, *func(sources))
            if func is terminal_join_func:
                targets = join.query(
                    select=select,
                    where=where,
                    order_by=order_by,
                    limit=limit,
                    offset=offset,
                )
            else:
                targets = join.query()

            field_value_2_targets = defaultdict(list)
            distinct_targets = set()

            evaluated_joins.append(join)

            for bizobj in targets:
                target_field_value = bizobj[join.target_fname]
                field_value_2_targets[target_field_value].append(bizobj)
                distinct_targets.add(bizobj)

            for bizobj in sources:
                source_field_value = bizobj[join.source_fname]
                mapped_targets = field_value_2_targets.get(source_field_value)
                if mapped_targets:
                    paths[bizobj].extend(mapped_targets)

            sources = join.target_biz_type.BizList(distinct_targets)

        results = []
        terminal_biz_type = evaluated_joins[-1].target_biz_type

        for bizobj in original_sources:
            targets = self._get_terminal_nodes(
                paths, bizobj, terminal_biz_type, []
            )
            if self.many:
                results.append(terminal_biz_type.BizList(targets or []))
            else:
                results.append(targets[0] if targets else None)

        return results

    def _get_terminal_nodes(self, paths, parent, target_biz_type, acc):
        children = paths[parent]
        if not children and isinstance(parent, target_biz_type):
            acc.append(parent)
        else:
            for bizobj in children:
                self._get_terminal_nodes(paths, bizobj, target_biz_type, acc)
        return acc
