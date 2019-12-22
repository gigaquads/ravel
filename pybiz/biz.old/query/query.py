import random

from functools import reduce
from typing import List, Dict, Set, Text, Type, Tuple, Callable
from collections import defaultdict

from appyratus.enum import EnumValueStr
from appyratus.utils import DictUtils

import pybiz.biz

from pybiz.util.misc_functions import (
    is_sequence, is_biz_obj, is_biz_list, get_class_name
)
from pybiz.predicate import Predicate
from pybiz.schema import fields, StringTransformer
from pybiz.constants import ID_FIELD_NAME, REV_FIELD_NAME

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
        context: Dict = None
    ):
        self._alias = alias
        self._context = context if context is not None else {}

    @property
    def alias(self) -> Text:
        return self._alias

    @alias.setter
    def alias(self, alias: Text):
        if self._alias is not None:
            raise ValueError('alias is readonly')
        self._alias = alias

    @property
    def context(self) -> Dict:
        return self._context

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

    class Assignment(object):
        def __init__(self, name: Text, query: 'Query'):
            self.name = name
            self.query = query

        def __call__(self, value):
            self.query.params.custom[self.name] = value
            return self.query

    class Parameters(object):
        def __init__(
            self,
            fields=None,
            attributes=None,
            order_by=None,
            where=None,
            limit=None,
            offset=None,
            custom=None,
        ):
            self.fields = fields or {ID_FIELD_NAME: None, REV_FIELD_NAME: None}
            self.attributes = attributes or {}
            self.order_by = order_by or tuple()
            self.custom = custom or {}
            self.where = tuple()
            self.limit = None
            self.offset = None

        def keys(self):
            return set(self.fields.keys() | self.attributes.keys())


    loader = QueryLoader()
    executor = QueryExecutor()
    printer  = QueryPrinter()

    def __init__(
        self,
        biz_class: Type['BizType'],
        select: Set = None,
        where: Set = None,
        order_by: Tuple = None,
        limit: int = None,
        offset: int = None,
        custom: Dict = None,
        **kwargs,
    ):
        super().__init__(**kwargs)

        self._biz_class = biz_class
        self._params = Query.Parameters(custom=custom)
        self.select(biz_class.pybiz.default_selectors)

        if where is not None:
            self.where(where)
        if offset is not None:
            self.offset(offset)
        if limit is not None:
            self.limit(limit)
        if order_by is not None:
            self.order_by(order_by)
        if select is not None:
            self.select(select)

    def __getattr__(self, param_name):
        """
        This is so you can do query.foo('bar'), resulting in a 'bar': 'foo'
        entry in query.params.
        """
        return self.Assignment(param_name, self)

    def __getitem__(self, key):
        return getattr(self._params, key)

    def __setitem__(self, key, value):
        return setattr(self._params, key, value)

    def __repr__(self):
        biz_class_name = (
            get_class_name(self.biz_class) if self.biz_class else ''
        )
        if self.alias:
            alias_substr = f', alias="{self.alias}"'
        else:
            alias_substr = ''
        return f'<Query({biz_class_name}{alias_substr})>'

    def execute(
        self,
        first: bool = False,
        context: Dict = None,
        constraints: Dict[Text, 'Constraint'] = None,
        backfill: Backfill = None,
    ) -> 'BizThing':
        """
        Execute this Query, returning the target BizThing.
        """
        self.context.update(context or {})

        backfiller = QueryBackfiller() if backfill is not None else None

        targets = self.executor.execute(
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
        self._add_selectors(targets)
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
                elif isinstance(obj, Predicate):
                    additional_predicates.append(obj)
                else:
                    raise ValueError(f'unrecognized Predicate type: {obj}')

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
        self.printer.print_query(query=self)

    def dump(self):
        return {
            'class': get_class_name(self),
            'alias': self.alias,
            'limit': self.params.limit,
            'offset': self.params.offset,
            'order_by': [x.dump() for x in self.params.order_by],
            'where': [x.dump() for x in (self.params.where or [])],
            'target': {
                'type': get_class_name(self.biz_class),
                'fields': self.params.fields,
                'attributes': {
                    k: v.dump() for k, v
                    in self.params.attributes.items()
                }
            }
        }

    @property
    def biz_class(self) -> Type['BizObject']:
        return self._biz_class

    @property
    def params(self) -> Parameters:
        return self._params

    def _add_selectors(self, selectors):
        for selector in selectors:
            if is_sequence(selector):
                self._add_selectors(selector)
            else:
                self._add_selector(selector)

    def _add_selector(self, selector):
        """
        """
        # resolve pybiz type from string selector variable
        try:
            if isinstance(selector, str):
                selector = getattr(self._biz_class, selector)
        except AttributeError:
            raise AttributeError(
                f'{self._biz_class} has no attribute "{selector}"'
            )

        # add the selector to the appropriate collection
        if isinstance(selector, FieldProperty):
            assert selector.biz_class is self.biz_class
            # avoid the overhead of creating a FieldPropertyQuery when no
            # parameters are applied to this field query; hence, query is set
            # to None.
            query = None
            self._params.fields[selector.field.name] = query
        elif isinstance(selector, FieldPropertyQuery):
            assert selector.fprop.biz_class is self.biz_class
            self._params.fields[selector.alias] = selector
        elif isinstance(selector, (Query, BizAttributeQuery)):
            assert selector.alias in self.biz_class.pybiz.attributes
            self._params.attributes[selector.alias] = selector
        elif isinstance(selector, pybiz.biz.BizAttributeProperty):
            assert selector.biz_attr.biz_class is self.biz_class
            self._params.attributes[selector.biz_attr.name] = selector.select()
        elif isinstance(selector, type) and is_biz_obj(selector):
            # select everything but relationships
            biz_class = selector
            keys = (
                set(biz_class.pybiz.all_selectors) -
                biz_class.pybiz.attributes.relationships.keys()
            )
            for k in keys:
                self._params.fields[k] = None

        else:
            raise ValueError(f'unrecognized query selector: {selector}')

    @classmethod
    def load(cls, biz_class: Type['BizObject'], dumped: Dict) -> 'Query':
        return cls.loader.load(biz_class, dumped)

    @classmethod
    def load_from_keys(
        cls,
        biz_class: Type['BizObject'],
        keys: Set[Text] = None,
        _tree: Dict = None,
    ):
        keys = keys or biz_class.Schema.fields.keys()
        query = cls(biz_class)

        if _tree is None:
            assert keys
            _tree = DictUtils.unflatten_keys({k: None for k in keys})

        if '*' in _tree:
            del _tree['*']
            _tree.update({k: None for k in biz_class.Schema.fields})
        elif not _tree:
            _tree = {ID_FIELD_NAME: None, REV_FIELD_NAME: None}

        for k, v in _tree.items():
            if isinstance(v, dict):
                rel = biz_class.pybiz.attributes.relationships[k]
                sub_query = cls.load_from_keys(rel.target_biz_class, _tree=v)
                sub_query.alias = rel.name
                query._add_selector(sub_query)
            else:
                query._add_selector(k)

        return query


class FieldPropertyQuery(AbstractQuery):

    transformers = {
        fields.String: StringTransformer(),
    }

    def __init__(
        self,
        fprop: 'FieldProperty',
        params: Dict = None,
        callbacks: List = None,
        clean: bool = False,
        **kwargs
    ):
        super().__init__(**kwargs)
        self.fprop = fprop
        self.params = params or {}
        self.transformer = self._get_transformer()
        self.callbacks = callbacks or tuple()
        self.clean = clean

    def dump(self) -> Dict:
        record = {}
        record['alias'] = self.alias
        record['params'] = self.params.copy()
        record['field'] = self.fprop.field.name
        return record

    def execute(self, source: 'BizThing') -> 'BizThing':
        value = getattr(source, self.fprop.field.name)
        if value is not None:
            if self.params and (self.transformer is not None):
                for transform_name, arg in self.params.items():
                    value = self.transformer.transform(
                        transform_name, value, args=[arg]
                    )
        if self.clean:
            source.clean(self.fprop.field.name)
        for func in self.callbacks:
            value = func(source, value)
        return value

    def _get_transformer(self):
        if isinstance(self.fprop.field, pybiz.String):
            return self.transformers[pybiz.String]
        else:
            return None


class BizAttributeQuery(AbstractQuery):

    class Assignment(object):
        def __init__(self, name: Text, query: 'BizAttributeQuery'):
            self.name = name
            self.query = query

        def __call__(self, value):
            self.query.params[self.name] = value
            return self.query


    def __init__(
        self,
        biz_attr: 'BizAttribute',
        params: Dict = None,
        **kwargs
    ):
        super().__init__(**kwargs)
        self.params = params or {}
        self.biz_attr = biz_attr

    def __repr__(self):
        biz_class_name = (
            get_class_name(self.biz_attr.biz_class)
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
        entry in query.params.
        """
        return self.Assignment(param_name, self)

    def execute(self, source: 'BizObject'):
        biz_thing = self.biz_attr.execute(source, **self.params)
        return biz_thing

    def dump(self) -> Dict:
        record = {}
        record['class'] = get_class_name(self)
        record['alias'] = self.alias
        record['params'] = self.params.copy()
        record['target'] = {
            'attribute': self.biz_attr.name,
            'type': self.biz_attr.biz_class,
        },
        return record
