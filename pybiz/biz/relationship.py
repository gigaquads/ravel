import inspect

from copy import copy
from typing import Text, Type, Tuple, Dict, Set
from inspect import Parameter

from mock import MagicMock
from appyratus.memoize import memoized_property
from appyratus.schema.fields import Field

from pybiz.util import is_bizobj
from pybiz.predicate import (
    Predicate,
    ConditionalPredicate,
    BooleanPredicate,
    OP_CODE,
)

from .internal.query import QuerySpecification


class Relationship(object):
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
        conditions,
        on_set=None,
        on_get=None,
        on_del=None,
        on_add=None,
        many=False,
        ordering=None,
        fields=None,
        offset: int = None,
        limit: int = None,
        private=False,
        lazy=True,
        readonly=False,
    ):
        self.many = many
        self.private = private
        self.lazy = lazy
        self.readonly = readonly

        self.conditions = normalize_to_tuple(conditions)
        self.on_set = normalize_to_tuple(on_set)
        self.on_get = normalize_to_tuple(on_get)
        self.on_del = normalize_to_tuple(on_del)
        self.on_add = normalize_to_tuple(on_add)

        # set in self.bind. Host is the BizObject class that hosts tis
        # relationship, and `name` is the relationship attribute on said class.
        self._biz_type = None
        self._name = None

        self.metadata = []
        self._is_bootstrapped = False
        self._registry = None

        self._order_by = normalize_to_tuple(ordering)
        self._limit = max(1, limit) if limit is not None else None
        self._offset = max(0, offset) if offset is not None else None
        self._fields = fields

    def __repr__(self):
        return '<{rel_name}({attrs})>'.format(
            rel_name=self.__class__.__name__,
            attrs=', '.join([
                (self.biz_type.__name__ + '.' if self.biz_type else '')
                    + str(self._name) or '',
                'many={}'.format(self.many),
                'private={}'.format(self.private),
                'lazy={}'.format(self.lazy),
                'readonly={}'.format(self.readonly),
            ])
        )

    @property
    def name(self) -> Text:
        return self._name

    @property
    def join(self) -> Tuple:
        return self.conditions

    @property
    def biz_type(self) -> Type['BizObject']:
        return self._biz_type

    @property
    def target(self) -> Type['BizObject']:
        return self.metadata[-1]['target_type']

    @property
    def spec(self) -> 'QuerySpecification':
        return self._meta[-1]['spec']

    @property
    def is_bootstrapped(self):
        return self._is_bootstrapped

    @property
    def registry(self):
        return self._registry

    def on_bootstrap(self):
        pass

    def bootstrap(self, registry: 'Registry'):
        self._registry = registry

        # this injects all BizObject class names into the condition functions'
        # lexical scopes. This mechanism helps avoid cyclic import dependencies
        # for the sake of defining relationships in BizObjects.
        for func in self.conditions:
            func.__globals__.update(registry.manifest.types.biz)

        # resolve the BizObject classes to query in each condition and
        # prepare their QuerySpecifications.
        for idx, func in enumerate(self.conditions):
            sig = inspect.signature(func)

            mock_target = MagicMock()
            mock_kwargs = {
                k: MagicMock()
                for i, k in enumerate(sig.parameters) if i > 0
            }

            target_type, predicate = func(mock_target, **mock_kwargs)

            if func is self.conditions[0]:
                spec = QuerySpecification.prepare(self._fields, target_type)
                spec.limit = self._limit
                spec.offset = self._offset
                spec.order_by = self._order_by
                for order_by_func in spec.order_by:
                    order_by_obj = order_by_func()
                    spec.fields.add(order_by_obj.key)
            else:
                spec = QuerySpecification.prepare(None, target_type)

            self.metadata.append({
                'base_spec': spec,
                'target_type': target_type,
                'kwarg_names': {
                    p.name for i, p in enumerate(sig.parameters.values())
                    if (p.kind == Parameter.POSITIONAL_OR_KEYWORD) and (i > 0)
                },
            })

        self.on_bootstrap()
        self._is_bootstrapped = True

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
            target_type = meta['target_type']
            kwarg_names = meta['kwarg_names']
            spec = meta['base_spec']

            if kwargs and kwarg_names:
                func_kwargs = {k: kwargs.get(k) for k in kwarg_names}
            else:
                func_kwargs = kwargs

            if func is self.conditions[-1]:
                spec = copy(spec)
                if fields:
                    spec.fields = fields
                if limit is not None:
                    spec.limit = max(1, limit)
                if offset is not None:
                    spec.offset = max(0, offset)
                if ordering:
                    spec.order_by = normalize_to_tuple(ordering)

            predicate = func(target, **func_kwargs)[1]
            if predicate:
                result = target_type.query(predicate, specification=spec)
            else:
                result = None

            if not result:
                if self.many:
                    return caller.BizList([], self, caller)
                else:
                    return None
            else:
                target = result

        if self.many:
            result.relationship = self
            result.bizobj = caller   # TODO: rename bizobj back to owner

        return result

    def associate(self, biz_type: Type['BizObject'], name: Text):
        """
        This is called by the BizObject metaclass when associating its set of
        relationship objects with the owner BizObject class.
        """
        self._biz_type = biz_type
        self._name = name


def normalize_to_tuple(obj):
    if obj is not None:
        if isinstance(obj, (list, tuple)):
            return tuple(obj)
        return (obj, )
    return tuple()
