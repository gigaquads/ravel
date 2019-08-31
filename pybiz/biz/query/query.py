import random

from functools import reduce
from typing import List, Dict, Set, Text, Type, Tuple, Callable

from appyratus.enum import EnumValueStr

import pybiz.biz

from pybiz.util.misc_functions import is_sequence, is_bizobj, is_bizlist

from ..field_property import FieldProperty
from ..biz_list import BizList
from .order_by import OrderBy
from .query_loader import QueryLoader
from .query_executor import QueryExecutor
from .query_printer import QueryPrinter
from .query_backfiller import QueryBackfiller, Backfill


class AbstractQuery(object):
    def __init__(
        self,
        alias: Text = None,
        callbacks: List[Callable] = None,
    ):
        self._alias = alias
        self._callbacks = callbacks or {}

    @property
    def alias(self) -> Text:
        return self._alias

    @alias.setter
    def alias(self, alias: Text):
        if self._alias is not None:
            raise ValueError('alias is readonly')
        self._alias = alias

    @property
    def callbacks(self) -> List[Callable]:
        return self._callbacks

    def execute(self, source: 'BizThing') -> 'BizThing':
        raise NotImplementedError('override in subclass')


class Query(AbstractQuery):
    """
    query = (
        User.select(
            User.account.select(Account.name)
            User.email
        ).where(
            User.age > 14
        ).orderby(
            User.email.desc
        ).limit(1)
    )
    """

    class Parameters(object):
        def __init__(
            self,
            fields=None,
            attributes=None,
            order_by=None,
            where=None,
            limit=None,
            offset=None,
        ):
            self.fields = fields or {'_id': None, '_rev': None}
            self.attributes = attributes or {}
            self.order_by = order_by or tuple()
            self.where = tuple()
            self.limit = None
            self.offset = None

        def keys(self):
            return set(self.fields.keys() | self.attributes.keys())


    _loader = QueryLoader()
    _executor = QueryExecutor()
    _printer  = QueryPrinter()

    def __init__(
        self,
        biz_class: Type['BizType'],
        alias: Text = None,
        select: Set = None,
        order_by: Tuple = None,
        limit: int = None,
        offset: int = None,
        callbacks: List[Callable] = None,
    ):
        super().__init__(alias=alias, callbacks=callbacks)

        self._biz_class = biz_class
        self._params = Query.Parameters()

        self.select(biz_class.base_selectors)

        if offset is not None:
            self.offset(offset)
        if limit is not None:
            self.limit(limit)
        if order_by is not None:
            self.order_by(order_by)
        if select is not None:
            self.select(select)

    def __getitem__(self, key):
        return getattr(self._params, key)

    def __setitem__(self, key, value):
        return setattr(self._params, key, value)

    def __repr__(self):
        biz_class_name = self.biz_class.__name__ if self.biz_class else ''
        if self.alias:
            alias_substr = f', alias="{self.alias}"'
        else:
            alias_substr = ''
        return f'<Query({biz_class_name}{alias_substr})>'

    def execute(
        self,
        first: bool = False,
        constraints: Dict[Text, 'Constraint'] = None,
        backfill: Backfill = None,
    ) -> 'BizThing':
        """
        Execute this Query, returning the target BizThing.
        """
        backfiller = QueryBackfiller() if backfill is not None else None

        targets = self._executor.execute(
            query=self,
            backfiller=backfiller,
            constraints=constraints,
            first=first,
        )

        if backfill is None:
            targets.clean()
        elif backfill == Backfill.persistent:
            backfiller.persist()

        if first:
            return targets[0] if targets else None
        else:
            return targets

    def select(self, *targets) -> 'Query':
        self._add_targets(targets)
        return self

    def where(self, *predicates: 'Predicate', **kwargs) -> 'Query':
        """
        Append or replace "where"-expression Predicates.
        """
        if not (predicates or kwargs):
            return self
        else:
            additional_predicates = []
            for obj in predicates:
                if is_sequence(obj):
                    additional_predicates.extend(obj)
                else:
                    additional_predicates.append(obj)

            # in this contact, kwargs are interpreted as Equality predicates,
            # like user_id=1 would be interpreted as (User._id == 1)
            for k, v in kwargs.items():
                equality_predicate = (getattr(self.biz_class, k) == v)
                additional_predicates.append(equality_predicate)

            additional_predicates = tuple(additional_predicates)

            if self._params.where is None:
                self._params.where = tuple()

            self._params.where += additional_predicates

        return self

    def limit(self, limit: int) -> 'Query':
        """
        Set or re-set the Query limit int, for pagination. Used in convert with
        offset.
        """
        self._params.limit = max(limit, 1) if limit is not None else None
        return self

    def offset(self, offset: int) -> 'Query':
        """
        Set or re-set the Query offset int, for pagination, used in conjunction
        with limit.
        """
        self._params.offset = max(0, offset) if offset is not None else None
        return self

    def order_by(self, *order_by) -> 'Query':
        order_by_flattened = []
        for obj in order_by:
            if is_sequence(obj):
                order_by_flattened.extend(obj)
            else:
                order_by_flattened.append(obj)
        order_by_flattened = tuple(order_by_flattened)
        self._params.order_by = order_by_flattened
        return self

    def show(self):
        self._printer.print_query(query=self)

    def dump(self):
        return {
            'class': self.__class__.__name__,
            'alias': self.alias,
            'limit': self.params.limit,
            'offset': self.params.offset,
            'order_by': [x.dump() for x in self.params.order_by],
            'where': [x.dump() for x in (self.params.where or [])],
            'target': {
                'type': self.biz_class.__name__,
                'fields': self.params.fields,
                'attributes': {
                    k: v.dump() for k, v
                    in self.params.attributes.items()
                }
            }
        }

    @classmethod
    def load(cls, biz_class: Type['BizObject'], data: Dict) -> 'Query':
        return cls._loader.load(biz_class, data)

    @classmethod
    def from_keys(cls, biz_class: Type['BizObject'], keys: Set[Text] = None):
        if not keys:
            keys = biz_class.schema.fields.keys()
        return cls._loader.from_keys(biz_class, keys=keys)

    @property
    def biz_class(self) -> Type['BizObject']:
        return self._biz_class

    @property
    def executor(self) -> 'QueryExecutor':
        return self._executor

    @property
    def params(self) -> Parameters:
        return self._params

    def _add_targets(self, targets):
        for obj in targets:
            if is_sequence(obj):
                self._add_targets(obj)
            elif isinstance(obj, dict):
                for k, v in obj.items():
                    self._add_target(k, v)
            else:
                self._add_target(obj, None)

    def _add_target(self, target, params):
        """
        Add a new query target to this Query. A target can be a FieldProperty,
        RelationshipProperty, ViewProperty, or more generically, any
        BizAttribute declared on self.biz_class, the targeted BizObject class.
        The target can also just be the string name of one of these things.
        Finally, a target can also be an already-formed Query object.
        """
        key = None
        targets = None
        params = params if params is not None else {}

        # resolve pybiz type from string target variable
        try:
            if isinstance(target, str):
                target = getattr(self._biz_class, target)
        except AttributeError:
            raise AttributeError(
                f'{self._biz_class} has no attribute "{target}"'
            )

        # add the target to the appropriate collection
        if isinstance(target, FieldProperty):
            assert target.biz_class is self.biz_class
            key = target.field.name
            targets = self._params.fields
        elif isinstance(target, pybiz.biz.BizAttributeProperty):
            assert target.biz_attr.biz_class is self.biz_class
            biz_attr = target.biz_attr
            key = biz_attr.name
            targets = self._params.attributes
            if biz_attr.category == 'relationship':
                params = Query.from_keys(biz_class=biz_attr.target_biz_class)
        elif isinstance(target, (Query, BizAttributeQuery)):
            assert target.alias in self.biz_class.attributes
            key = target.alias
            targets = self._params.attributes
            params = target

        if targets is not None:
            targets[key] = params


class BizAttributeQuery(AbstractQuery):

    class Assignment(object):
        def __init__(self, name: Text, query: 'BizAttributeQuery'):
            self.name = name
            self.query = query

        def __call__(self, value):
            self.query._params[self.name] = value
            return self.query


    def __init__(self, biz_attr: 'BizAttribute', alias=None, *args, **kwargs):
        super().__init__(alias=alias)
        self._biz_attr = biz_attr
        self._params = {}

    def __repr__(self):
        biz_class_name = (
            self._biz_attr.biz_class.__name__
            if self.biz_attr else ''
        )
        if self.alias:
            alias_substr = f', alias="{self.alias}"'
        else:
            alias_substr = ''

        return f'<BizAttributeQuery({biz_class_name}{alias_substr})>'

    def __getattr__(self, param_name):
        """
        This is so you can do query.foo('bar'), resulting in a 'bar': 'foo'
        entry in query._params.
        """
        return BizAttributeQuery.Assignment(param_name, self)

    @property
    def biz_attr(self) -> 'BizAttribute':
        return self._biz_attr

    @property
    def params(self) -> Dict:
        return self._params

    def execute(self, source: 'BizObject'):
        biz_thing = self._biz_attr.execute(source, **self._params)
        return biz_thing

    def dump(self) -> Dict:
        record = self.params.copy()
        record['class'] = self.__class__.__name__
        record['alias'] = self.alias
        record['target'] = {
            'attribute': self.biz_attr.name,
            'type': self.biz_attr.biz_class,
        }
        return record
