import sys

from typing import Text, Type, Dict, Callable

from appyratus.schema import Schema
from appyratus.enum import EnumValueStr, EnumValueInt

from ..biz_thing import BizThing
from ..query import BizAttributeQuery


class BizAttribute(object):
    """
    BizAttributes are extensible, query-able attributes that can bet set on
    BizObjects, whose data can be accessed through properties that are
    automatically built at runtime. More details to come...
    """

    class PybizGroup(EnumValueStr):
        """
        BizAttribute "group" enum values for subclasses built into Pybiz.
        """

        @staticmethod
        def values():
            return {
                'relationship',
                'view',
            }


    class PybizPriority(EnumValueInt):
        """
        BizAttribute "priority" enum values for subclasses built into Pybiz.
        """

        @staticmethod
        def values():
            return {
                'relationship': 1,
                'view': 10,
            }


    def __init__(
        self,
        name: Text = None,
        private: bool = None,
        lazy: bool = True,
        *args, **kwargs
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

    def __lt__(self, other: 'BizAttribute'):
        return self.priority < other.priority

    def __leq__(self, other: 'BizAttribute'):
        return self.priority <= other.priority

    def __gt__(self, other: 'BizAttribute'):
        return self.priority > other.priority

    def __geq__(self, other: 'BizAttribute'):
        return self.priority >= other.priority

    def __eq__(self, other: 'BizAttribute'):
        return self.priority == other.priority

    def __neq__(self, other: 'BizAttribute'):
        return self.priority != other.priority

    def build_property(self) -> 'BizAttributeProperty':
        return BizAttributeProperty(self)

    @property
    def is_relationship(self) -> bool:
        return self.group == self.PybizGroup.relationship

    @property
    def is_view(self) -> bool:
        return self.group == self.PybizGroup.view

    @property
    def priority(self) -> int:
        return sys.maxsize

    @property
    def group(self) -> Text:
        return ''

    @property
    def name(self) -> Text:
        return self._name

    @name.setter
    def name(self, name: Text):
        self._name = name

    @property
    def private(self) -> bool:
        return self._private

    @private.setter
    def private(self, private: bool):
        self._private = private

    @property
    def lazy(self) -> Text:
        return self._lazy

    @lazy.setter
    def lazy(self, lazy: bool):
        self._lazy = lazy

    @property
    def is_bootstrapped(self) -> bool:
        return self._is_bootstrapped

    @property
    def biz_class(self) -> Type['BizObject']:
        return self._biz_class

    @property
    def app(self) -> 'Application':
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

    def execute(self, source: 'BizObject', *args, **kwargs) -> object:
        raise NotImplementedError()

    def generate(self, source: 'BizObject', *args, **kwargs) -> object:
        raise NotImplementedError()


class BizAttributeProperty(property):
    def __init__(self, biz_attr: 'BizAttribute'):
        super().__init__(fset=self.fset, fget=self.fget, fdel=self.fdel)
        self._biz_attr = biz_attr

    def __repr__(self):
        name = f'{self.biz_attr.name}: ' if self.biz_attr.name else ''
        biz_attr_class = self.biz_attr.__class__.__name__
        return f'<BizAttributeProperty({name}{biz_attr_class})>'

    def select(self, *args, **kwargs) -> 'BizAttributeQuery':
        return BizAttributeQuery(
            biz_attr=self.biz_attr,
            alias=self.biz_attr.name,
            *args, **kwargs
        )

    @property
    def biz_attr(self) -> 'BizAttribute':
        return self._biz_attr

    def fget(self, bizobj: 'BizObject', *execute_args, **execute_kwargs):
        key = self.biz_attr.name
        is_loaded = key in bizobj.internal.attributes
        if (not is_loaded) and self.biz_attr.lazy:
            value = self.biz_attr.execute(
                bizobj, *execute_args, **execute_kwargs
            )
            bizobj.internal.attributes[key] = value
        return bizobj.internal.attributes.get(key)

    def fset(self, bizobj: 'BizObject', value):
        bizobj.internal.attributes[self.biz_attr.name] = value

    def fdel(self, bizobj: 'BizObject'):
        bizobj.internal.attributes.pop(self.biz_attr.name, None)
