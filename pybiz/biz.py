import re
import os

from abc import ABCMeta, abstractmethod
from types import MethodType

from .patch import JsonPatchMixin
from .dao import DaoManager
from .const import (
    PRE_PATCH_ANNOTATION,
    POST_PATCH_ANNOTATION,
    PATCH_PATH_ANNOTATION,
    PATCH_ANNOTATION,
    )


class BizObjectMeta(ABCMeta):
    def __init__(cls, name, bases, dict_):
        # set this attribute in order to be able to
        # use duck typing to check isinstance of BizObjects
        setattr(cls, 'is_bizobj', True)

        # build field properties according to the schema
        # associated with this BizObject class
        schema_factory = cls.schema()
        if schema_factory is not None:
            s = cls._schema = schema_factory()
            s.strict = True
            if s is not None:
                cls.build_properties(s)

        # JsonPatchMixin integration:
        # register pre and post patch callbacks
        if any(issubclass(x, JsonPatchMixin) for x in bases):
            # scan class methods for those annotated as patch hooks
            # and register them as such.
            for k in cls.__dict__:
                v = getattr(cls, k)
                if isinstance(v, MethodType):
                    path = getattr(v, PATCH_PATH_ANNOTATION, None)
                    if hasattr(v, PRE_PATCH_ANNOTATION):
                        assert path
                        cls.add_pre_patch_hook(path, k)
                    elif hasattr(v, PATCH_ANNOTATION):
                        assert path
                        cls.set_patch_hook(path, k)
                    elif hasattr(v, POST_PATCH_ANNOTATION):
                        assert path
                        cls.add_post_patch_hook(path, k)

        ABCMeta.__init__(cls, name, bases, dict_)

    def build_properties(cls, schema):
        """
        Create properties out of the fields declared on the schema associated
        with the class.
        """
        def build_property(k):
            def fget(self):
                return self[k]

            def fset(self, value):
                self[k] = value

            def fdel(self):
                del self[k]

            return property(fget=fget, fset=fset, fdel=fdel)

        for field_name in schema.fields:
            assert not hasattr(cls, field_name)
            setattr(cls, field_name, build_property(field_name))


class BizObject(JsonPatchMixin, metaclass=BizObjectMeta):

    _schema = None  # auto-memoized Schema instance
    _dao_manager = DaoManager.get_instance()

    def __init__(self, data=None, mark_dirty=True, **kwargs_data):
        data = data or {}
        data.update(kwargs_data)
        data.setdefault('_id', None)
        data.setdefault('public_id', None)
        if self._schema is not None:
            result = self._schema.load(data)
            if result.errors:
                raise Exception(str(result.errors))
            self._data = result.data
        else:
            self._data = data

        self._dirty_set = set()
        if mark_dirty:
            self.mark_dirty()

    def __getitem__(self, attr):
        return self._data[attr]

    def __setitem__(self, attr, value):
        if self._schema is not None:
            if attr not in self._schema.fields:
                raise KeyError('{} not in {} schema'.format(
                        attr, self._schema.__class__.__name__))
        self._data[attr] = value

    def __contains__(self, key):
        return key in self._data

    def __repr__(self):
        bizobj_id = ''
        if self._data['_id'] is not None:
            bizobj_id = '/id={}'.format(self._data['_id'])
        elif self._data['public_id'] is not None:
            bizobj_id = '/pid={}'.format(self._data['public_id'])
        dirty_flag = '*' if self._dirty_set else ''
        return '<{class_name}{dirty_flag}{bizobj_id}>'.format(
                class_name=self.__class__.__name__,
                bizobj_id=bizobj_id,
                dirty_flag=dirty_flag)

    @classmethod
    @abstractmethod
    def schema(cls):
        return None

    @classmethod
    def dao_provider(cls):
        name = re.sub(r'([a-z])([A-Z0-9])', r'\1_\2', cls.__name__).upper()
        provider = os.environ.get('{}_DAO_PROVIDER'.format(name))
        return provider or os.environ['DAO_PROVIDER']

    @classmethod
    def get_dao(cls):
        return cls._dao_manager.get_dao_for_bizobj(cls)

    @property
    def dao(self):
        return self._dao_manager.get_dao_for_bizobj(self.__class__)

    def keys(self):
        return self._data.keys()

    def values(self):
        return self._data.values()

    def items(self):
        return self._data.items()

    def update(self, dct):
        self._data.update(dct)

    def dump(self, schema=None):
        schema = schema or self._schema
        if schema is not None:
            return schema.dump(self._data).data
        else:
            return self._data.copy()

    @property
    def dirty(self):
        return frozenset(self._dirty_set)

    def mark_dirty(self, attr=None):
        if attr is not None:
            assert attr in self._data
            self._dirty_set.add(attr)
        else:
            self._dirty_set = set(self._data.keys())

    def save(self, and_fetch=False):
        bizobjs = []
        data = {}  # data to save

        # accumulate all nested bizobjs in order to call save
        # recursively on them. at the same time, build up the
        # data structure(s) passed down to the DAL.
        for k in self._dirty_set:
            v = self._data[k]
            if isinstance(v, BizObject):
                bizobjs.append(v)
            else:
                data[k] = v

        # depth-first save of nested bizobjs
        for bizobj in bizobjs:
            bizobj.save()

        # persist data and update this bizobj
        if data:
            # TODO: pass both the old and the new value to dao.save
            # so that it can figure out optimally what to save.
            _id = self.dao.save(data, _id=self._id)
            self._id = _id
            if and_fetch:
                self.update(self.dao.fetch(_id=_id))
            self._dirty_set.clear()
