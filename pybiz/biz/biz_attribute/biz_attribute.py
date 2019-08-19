import sys

from typing import Text, Type, Dict, Callable

from appyratus.schema import Schema

import pybiz.biz.query


class BizAttribute(object):
    def __init__(
        self,
        name: Text = None,
        private: bool = None,
        lazy: bool = True,
        *args,
        **kwargs
    ):
        self._name = name
        self._private = private
        self._lazy = lazy

        # vars set by bootstrap:
        self._is_bootstrapped = False
        self._app = None
        self._biz_class = None

    def __repr__(self):
        name = f'{self.name}:' if self.name else ''
        biz_attr_class = self.__class__.__name__
        return f'<BizAttribute({name}{biz_attr_class})>'

    def __lt__(self, other):
        return self.order_key < other.order_key

    def __leq__(self, other):
        return self.order_key <= other.order_key

    def __gt__(self, other):
        return self.order_key > other.order_key

    def __geq__(self, other):
        return self.order_key >= other.order_key

    def __eq__(self, other):
        return self.order_key == other.order_key

    def __neq__(self, other):
        return self.order_key != other.order_key

    def build_property(self) -> 'BizAttributeProperty':
        return BizAttributeProperty(self)

    @property
    def order_key(self):
        return sys.maxsize

    @property
    def category(self):
        return 'biz_attribute'

    @property
    def name(self) -> Text:
        return self._name

    @name.setter
    def name(self, name):
        self._name = name

    @property
    def private(self):
        return self._private

    @private.setter
    def private(self, private):
        self._private = private

    @property
    def lazy(self) -> Text:
        return self._lazy

    @lazy.setter
    def lazy(self, lazy):
        self._lazy = lazy

    @property
    def is_bootstrapped(self):
        return self._is_bootstrapped

    @property
    def biz_class(self) -> Type['BizObject']:
        return self._biz_class

    @property
    def app(self):
        return self._app

    def bootstrap(self, app: 'Application'):
        self._app = app
        self.on_bootstrap()
        self._is_bootstrapped = True

    def on_bootstrap(self):
        pass

    def bind(self, biz_class: Type['BizObject']):
        """
        This is called by the BizObject metaclass when associating its set of
        BizAttribute objects with the owner BizObject class.
        """
        self._biz_class = biz_class

    def execute(self, source: 'BizObject', *args, **kwargs):
        return


class BizAttributeProperty(property):
    def __init__(self, biz_attr: 'BizAttribute'):
        super().__init__(fset=self.fset, fget=self.fget, fdel=self.fdel)
        self._biz_attr = biz_attr

    def __repr__(self):
        name = f'{self.biz_attr.name}: ' if self.biz_attr.name else ''
        biz_attr_class = self.biz_attr.__class__.__name__
        return f'<BizAttributeProperty({name}{biz_attr_class})>'

    def select(self, *args, **kwargs) -> 'BizAttributeQuery':
        return pybiz.biz.query.BizAttributeQuery(
            biz_attr=self.biz_attr,
            alias=self.biz_attr.name,
            *args, **kwargs
        )

    @property
    def biz_attr(self) -> 'BizAttribute':
        return self._biz_attr

    def fget(self, bizobj: 'BizObject', *execute_args, **execute_kwargs):
        key = self.biz_attr.name
        is_loaded = key in bizobj.internal.cache
        if (not is_loaded) and self.biz_attr.lazy:
            value = self.biz_attr.execute(
                bizobj, *execute_args, **execute_kwargs
            )
            bizobj.internal.cache[key] = value
        return bizobj.internal.cache.get(key)

    def fset(self, bizobj: 'BizObject', value):
        bizobj.internal.cache[self.biz_attr.name] = value

    def fdel(self, bizobj: 'BizObject'):
        bizobj.internal.cache.pop(self.biz_attr.name, None)
