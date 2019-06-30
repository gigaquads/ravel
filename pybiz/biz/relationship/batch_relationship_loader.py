import inspect

from copy import copy
from typing import Text, Type, Tuple, Dict, Set, List
from inspect import Parameter
from collections import defaultdict

from mock import MagicMock
from appyratus.memoize import memoized_property
from appyratus.schema.fields import Field

from pybiz.util import is_bizobj, normalize_to_tuple
from pybiz.exc import RelationshipArgumentError
from pybiz.predicate import (
    Predicate,
    ConditionalPredicate,
    BooleanPredicate,
    OP_CODE,
)

from pybiz.biz.internal.query import QuerySpecification


class BatchRelationshipLoader(object):
    def __init__(self, conditions, order_by=None, many=False):
        self._conditions = normalize_to_tuple(conditions)
        self._order_by = order_by
        self._many = many

    def load(self, callers: List['BizObject'], fields = None, args: Dict = None):
        if not callers:
            return

        sources = callers
        args = args or {}
        order_by = self._order_by() if self._order_by else None
        terminal_target_type = None

        for idx, func in enumerate(self._conditions):
            source_field_prop, target_field_prop, query_predicate = func(sources, **args)

            source_field_name = source_field_prop.field.name
            target_field_name = target_field_prop.field.name
            target_type = target_field_prop.target
            order_by = order_by if idx == len(self._conditions) - 1 else None

            if idx == len(self._conditions) - 1:
                terminal_target_type = target_type

            targets = target_type.query(
                predicate=query_predicate,
                order_by=order_by,
                fields=fields,
            )

            target_map = defaultdict(list)
            distinct_targets = set()

            for target_bizobj in targets:
                target_field_value = target_bizobj[target_field_name]
                target_map[target_field_value].append(target_bizobj)
                distinct_targets.add(target_bizobj)

            for source_bizobj in sources:
                source_field_value = source_bizobj[source_field_name]
                mapped_targets = target_map.get(source_field_value)

                if idx == 0:
                    source_bizobj._children = list(mapped_targets)
                else:
                    source_bizobj._children.extend(mapped_targets)

            sources = target_type.BizList(distinct_targets)

        def get_terminal_nodes(parent, acc):
            children = getattr(parent, '_children', None)
            if not children:
                acc.append(parent)
            else:
                for bizobj in children:
                    get_terminal_nodes(bizobj, acc)
                delattr(parent, '_children')
            return acc

        results = []

        for caller in callers:
            targets = get_terminal_nodes(caller, [])
            if self._many:
                #if targets and targets[0].__class__ != target_field.biz_type:
                    # ensure targets are of the expected terminal type
                #    targets = []
                results.append(terminal_target_type.BizList(targets or []))
            else:
                results.append(targets[0] if targets else None)

        return results
