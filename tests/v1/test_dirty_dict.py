from appyratus.test import mark
from ravel.util.dirty import DirtyDict


class TestDirtyDict(object):

    @mark.unit
    def test_dirty_set(cls):
        d1 = DirtyDict()
        assert not d1.dirty

        d1['a'] = 1
        assert d1.dirty
        assert d1.dirty == {'a'}

    @mark.unit
    def test_nested_dirty_set(cls):
        d1 = DirtyDict(a=DirtyDict(b=1))
        d1.clear_dirty()
        d1['a'].clear_dirty()
        assert not d1.dirty
        assert not d1['a'].dirty

        d1['a']['b'] = 2
        assert d1['a'].dirty
        assert d1.dirty

    @mark.unit
    def test_nested_dirty_set_with_setitem(cls):
        d1 = DirtyDict()
        d2 = DirtyDict(b=1)
        d1.clear_dirty()
        d2.clear_dirty()
        assert not d1.dirty
        assert not d2.dirty

        d1['a'] = d2
        assert d1.dirty

        d1.clear_dirty()
        assert not d1.dirty

        d1['a']['b'] = 2
        assert d1 == d1['a']._parent_ref()
        d2.has_parent(d1)

        assert d1['a']._key_in_parent == 'a'
        assert d1['a'].dirty
        assert d1.dirty

    @mark.unit
    def test_dirty_set_with_non_empty_ctor(cls):
        d1 = DirtyDict({'a': 1})
        assert d1.dirty == {'a'}

        d1 = DirtyDict({'a': 1})
        d1.clear_dirty()
        assert d1.dirty == set()

    @mark.unit
    def test_clear_dirty(cls):
        d1 = DirtyDict()
        d1['a'] = 1
        assert 'a' in d1.dirty
        d1.clear_dirty()
        assert not d1.dirty

    @mark.unit
    def test_recursive_init(cls):
        data = {'a': {'b': 'c'}}
        d = DirtyDict(data, recursive=True)
        assert isinstance(d['a'], DirtyDict)
