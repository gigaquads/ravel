from typing import Text, Type, Dict, Callable

from appyratus.schema import Schema


class BizAttribute(object):
    def __init__(self, name: Text = None, private=False, *args, **kwargs):
        self._name = name
        self._private = private
        self._is_bootstrapped = False
        self._registry = None
        self._biz_type = None

    @property
    def name(self) -> Text:
        return self._name

    @property
    def private(self):
        return self._private

    @property
    def is_bootstrapped(self):
        return self._is_bootstrapped

    @property
    def biz_type(self) -> Type['BizObject']:
        return self._biz_type

    @property
    def registry(self):
        return self._registry

    def associate(self, biz_type: Type['BizObject'], name: Text):
        """
        This is called by the BizObject metaclass when associating its set of
        BizAttribute objects with the owner BizObject class.
        """
        self._biz_type = biz_type
        self._name = name

    def bootstrap(self, registry: 'Registry'):
        self._registry = registry
        self.on_bootstrap()
        self._is_bootstrapped = True

    def on_bootstrap(self):
        pass
