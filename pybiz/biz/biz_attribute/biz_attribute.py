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

    @classmethod
    def bless(
        cls,
        blessed_biz_class: Type['BizObject'],
        source_biz_attr: 'BizAttribute'
    ) -> 'BizAttribute':
        """
        Bless a "blessed" BizObject class with its own copy of a given "source"
        BizAttribute, registering it with the blessed AttributeManager and
        bootstrapping it if the source itself is bootstrapped.
        """
        # TODO: make copy an instance method
        biz_attr_copy = source_biz_attr.copy()
        biz_attr_copy._biz_class = blessed_biz_class

        if source_biz_attr.is_bootstrapped:
            # NOTE: if it's not bootstrapped, it's ok because it means that the
            # host Application hasn't bootstrapped completely yet, implying that
            # this copy and the original BizAttribute will be bootstrapped when
            # the App eventually does bootstrap.
            blessed_biz_class.bootstrap(source_biz_attr.app)

        blessed_biz_class.pybiz.attributes.register(
            source_biz_attr.name, biz_attr_copy
        )

        return biz_attr_copy

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

    def copy(self) -> 'BizAttribute':
        raise NotImplementedError()

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

    def fget(self, biz_obj: 'BizObject', *execute_args, **execute_kwargs):
        key = self.biz_attr.name
        is_loaded = key in biz_obj.internal.attributes
        if (not is_loaded) and self.biz_attr.lazy:
            value = self.biz_attr.execute(
                biz_obj, *execute_args, **execute_kwargs
            )
            biz_obj.internal.attributes[key] = value
        return biz_obj.internal.attributes.get(key)

    def fset(self, biz_obj: 'BizObject', value):
        biz_obj.internal.attributes[self.biz_attr.name] = value

    def fdel(self, biz_obj: 'BizObject'):
        biz_obj.internal.attributes.pop(self.biz_attr.name, None)
