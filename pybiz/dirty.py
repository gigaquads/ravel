import weakref
import uuid

from abc import ABCMeta, abstractmethod


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


class DirtyDict(dict, DirtyInterface):

    def __init__(self, data=None, **kwargs):
        super(DirtyDict, self).__init__(data or {}, **kwargs)
        self._hash = int(uuid.uuid4().hex, 16)
        self._dirty_keys = set(self.keys())
        self._parent_ref = None
        self._key_in_parent = None

        for k, v in self.items():
            if isinstance(v, DirtyInterface):
                v.set_parent(k, self)

    def __hash__(self):
        """This is needed to be able to store instances as weakrefs."""
        return self._hash

    def __setitem__(self, key, value):
        super(DirtyDict, self).__setitem__(key, value)
        self.mark_dirty(key)
        if isinstance(value, DirtyInterface):
            value.set_parent(key, self)

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

    def mark_dirty(self, key):
        self._dirty_keys.add(key)
        if self._parent_ref is not None:
            parent = self._parent_ref()
            if parent is not None:
                parent.mark_dirty(self._key_in_parent)


class DirtyList(list, DirtyInterface):

    @property
    def dirty(self) -> frozenset:
        pass

    def set_parent(self, key_in_parent, parent) -> None:
        pass

    def has_parent(self, obj) -> bool:
        pass

    def mark_dirty(self, key) -> None:
        pass

    def clear_dirty(self) -> None:
        pass
