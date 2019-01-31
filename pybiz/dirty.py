import weakref
import uuid

from abc import ABCMeta, abstractmethod

from pybiz.util import is_sequence


class DirtyInterface(metaclass=ABCMeta):

    @property
    @abstractmethod
    def dirty(self) -> frozenset:
        pass

    @abstractmethod
    def set_parent(self, key_in_parent, parent) -> None:
        pass

    @abstractmethod
    def has_parent(self, obj) -> bool:
        pass

    @abstractmethod
    def get_parent(self) -> object:
        pass

    @abstractmethod
    def mark_dirty(self, key) -> None:
        pass

    @abstractmethod
    def clear_dirty(self, keys=None) -> None:
        pass


class DirtyObject(DirtyInterface):

    def __init__(self):
        super(DirtyObject, self).__init__()
        self._hash = int(uuid.uuid4().hex, 16)
        self._parent_ref = None
        self._key_in_parent = None
        self._dirty_keys = set()
        self.set_parent_on_children()
        self.initialize_dirty_children()

    @abstractmethod
    def set_parent_on_children(self):
        pass

    @abstractmethod
    def initialize_dirty_children(self, obj):
        pass

    def __hash__(self):
        return self._hash

    @property
    def key_in_parent(self):
        return self._key_in_parent

    @property
    def dirty(self):
        return frozenset(self._dirty_keys)

    def set_parent(self, key_in_parent, parent):
        self._parent_ref = weakref.ref(parent)
        self._key_in_parent = key_in_parent

    def has_parent(self, obj):
        if self._parent_ref is not None:
            parent = self._parent_ref()
            if obj is parent:
                return True
        return False

    def get_parent(self):
        return self._parent_ref()

    def clear_dirty(self, keys=None):
        if keys is None:
            self._dirty_keys.clear()
        else:
            self._dirty_keys -= set(keys)

    def mark_dirty(self, keys):
        if not isinstance(keys, set):
            keys = {keys} if isinstance(keys, (int, str)) else set(keys)
        self._dirty_keys |= keys
        if self._parent_ref is not None:
            parent = self._parent_ref()
            if parent is not None:
                parent.mark_dirty(self._key_in_parent)


class DirtyDict(dict, DirtyObject):

    def __init__(self, data=None, **kwargs):
        dict.__init__(self, data or {}, **kwargs)
        DirtyObject.__init__(self)
        self.mark_dirty(self.keys())

    def __setitem__(self, key, value):
        super(DirtyDict, self).__setitem__(key, value)
        self.mark_dirty(key)
        if isinstance(value, DirtyInterface):
            value.set_parent(key, self)

    def set_parent_on_children(self):
        for k, v in self.items():
            if isinstance(v, DirtyInterface):
                v.set_parent(k, self)

    def initialize_dirty_children(self):
        items = list(self.items())
        for k, v in items:
            if isinstance(v, dict):
                self[k] = DirtyDict(v)
            elif is_sequence(v):
                self[k] = DirtyList(v)


class DirtyList(list, DirtyObject):

    def __init__(self, data=None):
        list.__init__(self, data or [])
        DirtyObject.__init__(self)
        self.mark_dirty(range(len(self)))

    def __setitem__(self, idx, value):
        super(DirtyList, self).__setitem__(idx, value)
        self.mark_dirty(idx)
        if isinstance(value, DirtyInterface):
            value.set_parent(idx, self)

    def set_parent_on_children(self):
        for i, v in enumerate(self):
            if isinstance(v, DirtyInterface):
                v.set_parent(i, self)

    def initialize_dirty_children(self):
        for i, v in enumerate(self):
            if isinstance(v, dict):
                self[i] = DirtyDict(v)
            elif is_sequence(v):
                self[i] = DirtyList(v)
