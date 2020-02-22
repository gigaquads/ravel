from ravel.util.misc_functions import (
    is_sequence,
)


class DirtyObject(object):
    def __init__(self, *args, **kwargs):
        self._dirty = set()

    @property
    def dirty(self):
        return self._dirty

    def clean(self, keys=None):
        if keys:
            for k in keys:
                self.dirty.discard(k)
        else:
            self.dirty.clear()

    def mark(self, keys=None):
        if keys:
            for k in keys:
                self.dirty.add(k)
        else:
            self.dirty.clear()


class DirtyDict(DirtyObject, dict):
    def __init__(self, *args, **kwargs):
        dict.__init__(self, *args, **kwargs)
        DirtyObject.__init__(self)
        self.dirty.update(self.keys())

    def __setitem__(self, key, value):
        super().__setitem__(key, value)
        self.dirty.add(key)

    def __delitem__(self, key):
        super().__delitem__(key)
        self.dirty.discard(key)

    def mark(self, keys=None):
        if not is_sequence(keys):
            keys = {keys}
        keys = {
            k for k in keys if k in self
        }
        super().mark(keys)

    def pop(self, key, default=None):
        value = super().pop(key, default)
        self.dirty.discard(key)
        return value

    def update(self, data):
        super().update(data)
        self.dirty.update(data.keys())

    def setdefault(self, key, default):
        if key not in self:
            self.dirty.add(key)
        return super().setdefault(key, default)


class DirtyList(DirtyObject, list):
    def __init__(self, *args, **kwargs):
        list.__init__(self, *args, **kwargs)
        DirtObject.__init__(self)
        self.dirty.update(range(len(self)))

    def __setitem__(self, index, value):
        super().__setitem__(index, value)
        self.dirty.add(index)

    def __delitem__(self, index):
        super().__delitem__(index)
        self.dirty.add(index)

    def append(self, value):
        super().append(value)
        self.dirty.add(len(self) - 1)

    def pop(self):
        value = super().pop()
        self.dirty.remove(len(self) - 1)
        return value
