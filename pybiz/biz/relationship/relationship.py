import inspect

from copy import copy
from typing import Text, Type, Tuple, Dict, Set, Callable
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

from pybiz.biz.internal.query import QuerySpecification

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

    def query(self):
        predicate = self._build_predicate()
        target = self.target_fprop.biz_type.query(predicate=predicate)
        return target

    def _build_predicate(self):
        target_field_value = getattr(self.source, self.source_fprop.field.name)
        computed_predicate = None
        if is_bizobj(self.source):
            computed_predicate = (self.target_fprop == target_field_value)
        elif is_bizlist(self.source):
            computed_predicate = (self.target_fprop.including(target_field_value))
        else:
            raise TypeError('TODO: raise custom exception')
        if self.predicate:
            computed_predicate &= self.predicate
        return computed_predicate



class Relationship(BizAttribute):
    def __init__(
        self,
        join: Tuple[Callable] = None,
        many=False,
        lazy=True,
    ):
        self.joins = normalize_to_tuple(join)
        self.many = many
        self.lazy = lazy
        self.target_biz_type = None
        self.on_get = tuple()

    def on_bootstrap(self):
        for func in self.joins:
            func.__globals__.update(self.registry.manifest.types.biz)
        self.target_biz_type = self.joins[-1](MagicMock())[1].biz_type

    def set_internally(self, owner: 'BizObject', related):
        # TODO: rename?
        owner.related[self.name] = related

    def query(self, source):
        if is_bizobj(source):
            return self._load_for_bizobj(source)
        else:
            return self._load_for_bizlist(source)

    def _load_for_bizobj(self, root: 'BizObject'):
        source = root
        target = None
        for func in self.joins:
            join = Join(source, *func(source))
            target = join.query()
            source = target
        if not self.many:
            return target[0] if target else None
        else:
            return target


    def _load_for_bizlist(self, sources: 'BizList'):
        if is_sequence(sources):
            sources = self.biz_type.BizList(sources)
        original_sources = sources
        joins = []
        for idx, func in enumerate(self.joins):
            join = Join(sources, *func(sources))
            targets = join.query()

            field_value_2_targets = defaultdict(list)
            distinct_targets = set()

            joins.append(join)

            for bizobj in targets:
                target_field_value = bizobj[join.target_fname]
                field_value_2_targets[target_field_value].append(bizobj)
                distinct_targets.add(bizobj)

            for bizobj in sources:
                source_field_value = bizobj[join.source_fname]
                mapped_targets = field_value_2_targets.get(source_field_value)
                if mapped_targets:
                    # TODO: don't modify the bizobj directly. use a temp var
                    if not hasattr(bizobj, '_children'):
                        bizobj._children = list(mapped_targets)
                    else:
                        bizobj._children.extend(mapped_targets)

            sources = join.target_biz_type.BizList(distinct_targets)

        results = []
        terminal_biz_type = joins[-1].target_biz_type
        for bizobj in original_sources:
            targets = self._get_terminal_nodes(bizobj, terminal_biz_type, [])
            if self.many:
                results.append(terminal_biz_type.BizList(targets or []))
            else:
                results.append(targets[0] if targets else None)

        return results

    def _get_terminal_nodes(self, parent, target_biz_type, acc):
        children = getattr(parent, '_children', [])
        if not children and isinstance(parent, target_biz_type):
            acc.append(parent)
        else:
            for bizobj in children:
                self._get_terminal_nodes(bizobj, target_biz_type, acc)
            if hasattr(parent, '_children'):
                delattr(parent, '_children')
        return acc


class _Relationship(BizAttribute):
    """
    `Relationship` objects are declared as `BizObject` class attributes and
    endow them with the ability to load and dump other `BizObject` objects and
    lists of objects recursively. For example,

    ```python3
    class Account(BizObject):

        # list of users at the account
        members = Relationship(
            conditions=(
                lambda self: (User, User.account_id == self._id)
            ), many=True
        )
    ```
    """

    def __init__(
        self,
        conditions=None,
        batch_conditions=None,
        on_set=None,
        on_get=None,
        on_del=None,
        on_add=None,
        on_rem=None,
        many=False,
        ordering=None,
        fields=None,
        offset: int = None,
        limit: int = None,
        private=False,
        lazy=True,
        readonly=False,
        behavior: 'RelationshipBehavior' = None,
    ):
        super().__init__(private=private)
        self.many = many
        self.lazy = lazy
        self.readonly = readonly
        if batch_conditions:
            self.batch_loader = BatchRelationshipLoader(
                conditions=batch_conditions,
                many=many,
            )
        else:
            self.batch_loader = None

        # relationship behavior provided, now process to generate conditions
        # and callbacks.  if any is not provided by the behavior, then default
        # to ones provided through this initialization
        if behavior is not None:
            self._behavior = behavior
            behavior_kwargs = behavior(relationship=self, many=many)
            conditions = behavior_kwargs.get('conditions', conditions)
            on_set = behavior_kwargs.get('on_set', on_set)
            on_get = behavior_kwargs.get('on_get', on_get)
            on_del = behavior_kwargs.get('on_del', on_del)
            on_add = behavior_kwargs.get('on_add', on_add)
            on_rem = behavior_kwargs.get('on_rem', on_rem)
        if behavior is None:
            self._behavior = None

        self.conditions = normalize_to_tuple(conditions)
        self.on_set = normalize_to_tuple(on_set)
        self.on_get = normalize_to_tuple(on_get)
        self.on_del = normalize_to_tuple(on_del)
        self.on_add = normalize_to_tuple(on_add)
        self.on_rem = normalize_to_tuple(on_rem)

        self.metadata = []

        self._ordering = ordering
        self._limit = max(1, limit) if limit is not None else None
        self._offset = max(0, offset) if offset is not None else None
        self._fields = fields

    def __repr__(self):
        return '<{rel_name}({attrs})>'.format(
            rel_name=self.__class__.__name__,
            attrs=', '.join(
                [
                    (self.biz_type.__name__ + '.' if self.biz_type else '') +
                    str(self.name) or '',
                    'many={}'.format(self.many),
                    'private={}'.format(self.private),
                    'lazy={}'.format(self.lazy),
                    'readonly={}'.format(self.readonly),
                ]
            )
        )

    @property
    def target(self) -> Type['BizObject']:
        return self.metadata[-1].target_type

    def pre_bootstrap(self):
        if self._behavior:
            self._behavior.pre_bootstrap()

    def on_bootstrap(self):
        self.pre_bootstrap()

        # this injects all BizObject class names into the condition functions'
        # lexical scopes. This mechanism helps avoid cyclic import dependencies
        # for the sake of defining relationships in BizObjects.
        registry = self._registry
        for func in self.conditions:
            func.__globals__.update(registry.manifest.types.biz)

        if self._ordering:
            self._ordering.__globals__.update(registry.manifest.types.biz)

        # analyze each Relationship condition function and collect relevant
        # metadata used in their incocation during self.query()
        for idx, func in enumerate(self.conditions):
            meta = ConditionMetadata(self, func).build()
            self.metadata.append(meta)
            # if limit, offset and other query specification arguments were
            # provided to the Relationship ctor and there are multiple
            # "condition" functions, then we need them to apply to the last
            # condition function to execute in self.query(), since these
            # parameters pertain to the target BizObject type, not to the
            # intermediate ones..
            if func is self.conditions[-1]:
                meta.query_spec.limit = self._limit
                meta.query_spec.offset = self._offset
                if self._ordering:
                    order_by = self._ordering(meta.target_type)
                    meta.query_spec.order_by = normalize_to_tuple(order_by)
                for x in meta.query_spec.order_by:
                    meta.query_spec.fields.add(x.key)

    def query(
        self,
        caller: 'BizObject',
        fields: Set[Text] = None,
        limit: int = None,
        offset: int = None,
        ordering: Tuple = None,
        kwargs: Dict = None,
    ):
        target = caller
        kwargs = kwargs or {}

        for func, meta in zip(self.conditions, self.metadata):
            # if this conditon func takes and keyword arguments, we need to
            # extract them by name from the kwargs parameter passed into this
            # method.
            if kwargs and meta.kwarg_names:
                func_kwargs = {k: kwargs.get(k) for k in meta.kwarg_names}
            else:
                func_kwargs = kwargs

            # if we're executing the final target condition function, we need to
            # update the QuerySpecification with any parameters passed in here
            # (like limit, offset, etc.)
            if func is self.conditions[-1]:
                spec = copy(meta.query_spec)
                if fields:
                    spec.fields.update(fields)
                if limit is not None:
                    spec.limit = max(1, limit)
                if offset is not None:
                    spec.offset = max(0, offset)
                if ordering:
                    spec.order_by = tuple(
                        func(meta.target_type)
                        for func in normalize_to_tuple(ordering)
                    )
            else:
                spec = meta.query_spec

            # since some condition function can take the host Relationship
            # as an initial positional argument and some may not, we have to
            # consider this here when invoking the function, which gives us
            # the query predicate to use in the query below.
            if meta.has_rel_argument:
                predicate = func(self, target, **func_kwargs)[1]
            else:
                predicate = func(target, **func_kwargs)[1]

            # finally perform the query.
            if predicate:
                result = meta.target_type.query(
                    predicate=predicate,
                    specification=spec,
                    first=(not self.many)
                )
            else:
                result = None

            # if we get an empty query result, we should abort further querying
            # and return an empty BizList if the relationship is "many" or null
            # otherwise.
            if not result:
                if self.many:
                    return caller.BizList([], self, caller)
                else:
                    return None
            else:
                target = result

        # we have a result, so we return it or only the first element if
        # first=True.
        if self.many:
            result.relationship = self
            result.bizobj = caller    # TODO: rename bizobj back to owner

        return result

    def set_internally(self, owner: 'BizObject', related):
        """
        This is somewhat hacky. It is used to perform on_set callbacks in
        otherwise readonly Relationships. This is necessary for relationships to
        be lazy loaded, for instance. We want readonly to mean only that the
        developer cannot explicitly assign to the relationship, not that the
        internal mechanisms should not work.
        """
        owner.related[self.name] = related
        for cb_func in self.on_set:
            cb_func(owner, related)


class ConditionMetadata(object):
    """
    This data class stores information regarding each "condition" function used
    in a Relationship. This data is used to decide what to do when executing the
    function in Relationship.query.
    """

    def __init__(self, relationship, func):
        self.func = func
        self.relationship = relationship
        self.signature = inspect.signature(func)
        self.has_rel_argument = False
        self.query_spec = None

    def build(self):
        self.num_positional_args = self._set_num_positional_args()
        self.has_rel_argument = self._set_has_rel_argument()
        self.kwarg_names = self._set_kwarg_names()
        self.target_type = self._set_target_type()
        # TODO: only get fields usedin relationshp query
        self.query_spec = QuerySpecification.build({'*'}, self.target_type)
        return self

    def _set_target_type(self):
        mock_self = MagicMock()
        mock_kwargs = {k: MagicMock() for k in self.kwarg_names}
        if self.has_rel_argument:
            cond_retval = self.func(
                self.relationship, mock_self, **mock_kwargs
            )
        else:
            cond_retval = self.func(mock_self, **mock_kwargs)
        if not isinstance(cond_retval, (list, tuple)):
            # a condition func must return Tuple[Type[BizObject], Predicate]
            raise ValueError(f'Unrecognized condition: {cond_retval}')
        target_type = cond_retval[0]
        return target_type

    def _set_kwarg_names(self):
        param_names = list(self.signature.parameters.keys())
        offset = 1 if not self.has_rel_argument else 2
        return set(param_names[offset:])

    def _set_num_positional_args(self):
        return len([
            p for p in self.signature.parameters.values()
            if p.kind == Parameter.POSITIONAL_OR_KEYWORD
        ])

    def _set_has_rel_argument(self):
        if self.num_positional_args == 1:
            return False
        elif self.num_positional_args == 2:
            return True
        else:
            raise RelationshipArgumentError(
                'condition functions do not accept '
                'custom positional arguments'
            )
