import sys

from typing import Text, Type, Dict, Callable

from appyratus.schema import Schema


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
        self._registry = None
        self._biz_type = None

    def __repr__(self):
        name = f'{self.name}:' if self.name else ''
        biz_attr_type = self.__class__.__name__
        return f'<BizAttribute({name}{biz_attr_type})>'

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
        if self._name is None:
            self._name = name
        else:
            raise ValueError('readonly')

    @property
    def private(self):
        return self._private

    @private.setter
    def private(self, private):
        if self._private is None:
            self._private = private
        else:
            raise ValueError('readonly')

    @property
    def lazy(self) -> Text:
        return self._lazy

    @lazy.setter
    def lazy(self, lazy):
        if self._lazy is None:
            self._lazy = lazy
        else:
            raise ValueError('readonly')

    @property
    def is_bootstrapped(self):
        return self._is_bootstrapped

    @property
    def biz_type(self) -> Type['BizObject']:
        return self._biz_type

    @property
    def registry(self):
        return self._registry

    def bootstrap(self, registry: 'Registry'):
        self._registry = registry
        self.on_bootstrap()
        self._is_bootstrapped = True

    def on_bootstrap(self):
        pass

    def bind(self, biz_type: Type['BizObject']):
        """
        This is called by the BizObject metaclass when associating its set of
        BizAttribute objects with the owner BizObject class.
        """
        self._biz_type = biz_type

    def execute(self, *args, **kwargs):
        return


class BizAttributeProperty(property):
    def __init__(self, biz_attr: 'BizAttribute'):
        super().__init__(fset=self.fset, fget=self.fget, fdel=self.fdel)
        self._biz_attr = biz_attr

    def __repr__(self):
        name = f'{self.biz_attr.name}:' if self.biz_attr.name else ''
        biz_attr_type = self.biz_attr.__class__.__name__
        return f'<BizAttributeProperty({name}{biz_attr_type})>'

    @property
    def biz_attr(self) -> 'BizAttribute':
        return self._biz_attr

    def fget(self, bizobj: 'BizObject', *execute_args, **execute_kwargs):
        key = self.biz_attr.name
        is_loaded = key in bizobj.memoized
        if (not is_loaded) and self.biz_attr.lazy:
            value = self.biz_attr.execute(
                bizobj, *execute_args, **execute_kwargs
            )
            bizobj.memoized[key] = value
        return bizobj.memoized.get(key)

    def fset(self, bizobj: 'BizObject', value):
        bizobj.memoized[self.biz_attr.name] = value

    def fdel(self, bizobj: 'BizObject'):
        bizobj.memoized.pop(self.biz_attr.name, None)
