import re
import os

from abc import ABCMeta, abstractmethod
from types import MethodType

from .patch import JsonPatchMixin
from .dao import DaoManager
from .dirty import DirtyDict, DirtyInterface
from .util import is_bizobj
from .const import (
    PRE_PATCH_ANNOTATION,
    POST_PATCH_ANNOTATION,
    PATCH_PATH_ANNOTATION,
    PATCH_ANNOTATION,
    IS_BIZOBJ_ANNOTATION,
    )


class BizObjectMeta(ABCMeta):
    def __init__(cls, name, bases, dict_):
        # set this attribute in order to be able to
        # use duck typing to check isinstance of BizObjects
        setattr(cls, IS_BIZOBJ_ANNOTATION, True)

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
            if field_name not in ('_id', 'public_id'):
                assert not hasattr(cls, field_name)
                setattr(cls, field_name, build_property(field_name))


class BizObject(DirtyInterface, JsonPatchMixin, metaclass=BizObjectMeta):

    _schema = None  # auto-memoized Schema instance
    _dao_manager = DaoManager.get_instance()

    def __init__(self, data=None, **kwargs_data):
        super(BizObject, self).__init__()
        self._data = self._init_data(data, kwargs_data)

    def _init_data(self, data, kwargs_data):
        data = data or {}
        data.update(kwargs_data)

        if self._schema is not None:
            result = self._schema.load(data)
            if result.errors:
                raise Exception(str(result.errors))
            data = result.data

        return DirtyDict(data)

    def __getitem__(self, key):
        return self._data[key]

    def __setitem__(self, key, value):
        if self._schema is not None:
            if key not in self._schema.fields:
                raise KeyError('{} not in {} schema'.format(
                        key, self._schema.__class__.__name__))
        self._data[key] = value

    def __contains__(self, key):
        return key in self._data

    def __repr__(self):
        bizobj_id = ''
        _id = self._data.get('_id')
        if _id is not None:
            bizobj_id = '/id={}'.format(_id)
        else:
            public_id = self._data.get('public_id')
            if public_id is not None:
                bizobj_id = '/public_id={}'.format(public_id)

        dirty_flag = '*' if self._data.dirty else ''

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
        """
        By default, we try to read the dao_provider string from an environment
        variable named X_DAO_PROVIDER, where X is the uppercase name of this
        class. Otherwise, we try to read a default global dao provider from the
        DAO_PROVIDER environment variable.
        """
        cls_name = re.sub(r'([a-z])([A-Z0-9])', r'\1_\2', cls.__name__).upper()
        dao_provider = os.environ.get('{}_DAO_PROVIDER'.format(cls_name))
        return dao_provider or os.environ['DAO_PROVIDER']

    @classmethod
    def get_dao(cls):
        return cls._dao_manager.get_dao_for_bizobj(cls)

    @property
    def data(self):
        return self._data

    @property
    def dao(self):
        return self._dao_manager.get_dao_for_bizobj(self.__class__)

    @property
    def _id(self):
        return self._data.get('_id')

    @_id.setter
    def _id(self, _id):
        self._data['_id'] = _id

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
        return self._data.dirty

    def set_parent(self, key_in_parent, parent):
        self._data.set_parent(key_in_parent, parent)

    def has_parent(self, obj):
        return self._data.has_parent(obj)

    def get_parent(self):
        return self._data.get_parent()

    def mark_dirty(self, key):
        self._data.mark_dirty(key)

    def clear_dirty(self, keys=None):
        self._data.clear_dirty(keys=keys)

    def save(self, fetch=False):
        nested_bizobjs = []
        data_to_save = {}

        # build data dict to save
        # and accumulated nested bizobjs
        for k in self._data.dirty:
            v = self._data[k]
            if is_bizobj(v):
                nested_bizobjs.append(v)
                data_to_save[k] = self._data[k].data
            else:
                data_to_save[k] = v

        # depth-first save nested bizobjs.
        for bizobj in nested_bizobjs:
            bizobj.save()

        # persist data and refresh data
        if data_to_save:
            _id = self.dao.save(data_to_save, _id=self._id)
            self._id = _id
            if fetch:
                self.update(self.dao.fetch(_id=_id))
            self.clear_dirty()
