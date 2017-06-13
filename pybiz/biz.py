"""
Relationship Load/Dump Mechanics:
    Load:
        - If any relationship name matches a schema field name
          of type <List> or <SubObject>, try to load the raw data
          into relationship data.
    Dump:
        - Simply dump the related objects into the data dict
          returned from the schema dump.

"""

import os
import copy
import re

from abc import ABCMeta, abstractmethod
from types import MethodType
from importlib import import_module

from .patch import JsonPatchMixin
from .dao import DaoManager
from .dirty import DirtyDict, DirtyInterface
from .util import is_bizobj
from .schema import Schema, Field
from .const import (
    PRE_PATCH_ANNOTATION,
    POST_PATCH_ANNOTATION,
    PATCH_PATH_ANNOTATION,
    PATCH_ANNOTATION,
    IS_BIZOBJ_ANNOTATION,
    )


class Relationship(object):

    def __init__(self, bizobj_class, many=False, dump_to=None, load_from=None):
        self.bizobj_class = bizobj_class
        self.load_from = load_from
        self.dump_to = dump_to
        self.many = many
        self.name = None

    def copy(self):
        return copy.copy(self)


class BizObjectMeta(ABCMeta):

    class RelationshipsMixin(object):
        def __init__(self, *args, **kwargs):
            self._relationship_data = {}

    def __new__(cls, name, bases, dict_):
        bases = bases + (cls.RelationshipsMixin,)
        new_class = ABCMeta.__new__(cls, name, bases, dict_)
        cls.add_is_bizobj_annotation(new_class)
        return new_class

    def __init__(cls, name, bases, dict_):
        ABCMeta.__init__(cls, name, bases + (cls.RelationshipsMixin,), dict_)
        relationships = cls.build_relationships()
        cls.build_all_properties(relationships)
        cls.register_JsonPatch_hooks(bases)

    def add_is_bizobj_annotation(new_class):
        # set this attribute in order to be able to
        # use duck typing to check isinstance of BizObjects
        setattr(new_class, IS_BIZOBJ_ANNOTATION, True)

    def build_all_properties(cls, relationships):
        # NOTE: the names of Fields declared in the Schema and Relationships
        # declared on the BizObject will overwrite any methods or attributes
        # defined explicitly on the BizObject class. This happens below.
        #
        cls.build_relationship_properties(relationships)
        cls._relationships = relationships

        schema_retval = cls.__schema__()
        if isinstance(schema_retval, str):
            schema_class = cls.import_schema_class(cls.__schema__())
        else:
            schema_class = schema_retval

        if schema_class is not None:
            s = cls._schema = schema_class()
            s.strict = True
            if s is not None:
                cls.build_field_properties(s, relationships)

    def import_schema_class(self, class_path_str):
        class_path = class_path_str.split('.')
        assert len(class_path) > 1

        module_path_str = '.'.join(class_path[:-1])
        class_name = class_path[-1]

        schema_module = import_module(module_path_str)
        schema_class = getattr(schema_module, class_name)

        return schema_class

    def register_JsonPatch_hooks(cls, bases):
        if not any(issubclass(x, JsonPatchMixin) for x in bases):
            return
        # scan class methods for those annotated as patch hooks
        # and register them as such.
        for k in dir(cls):
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

    def build_relationships(cls):
        # aggregate all relationships delcared on the bizobj
        # class into a single "relationships" dict.
        direct_relationships = {}
        inherited_relationships = {}

        for k in dir(cls):
            rel = getattr(cls, k)
            is_relationship = isinstance(rel, Relationship)
            if not is_relationship:
                is_super_relationship = k in cls._relationships
                if is_super_relationship:
                    super_rel = cls._relationships[k]
                    rel = super_rel.copy()
                    rel.name = k
                    inherited_relationships[k] = rel
            else:
                direct_relationships[k] = rel
                rel.name = k

        # clear the Relationships delcared on this subclass
        # from the class name space, to be replaced dynamically
        # with properties later on.
        for k in direct_relationships:
            delattr(cls, k)

        inherited_relationships.update(direct_relationships)
        return inherited_relationships

    def build_field_properties(cls, schema, relationships):
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
            if field_name not in relationships:
                setattr(cls, field_name, build_property(field_name))

    def build_relationship_properties(cls, relationships):
        def build_rel_property(k, rel):
            def fget(self):
                return self._relationship_data.get(k)

            def fset(self, value):
                rel = self._relationships[k]
                is_sequence = isinstance(value, (list, tuple, set))
                if not is_sequence:
                    if rel.many:
                        raise ValueError('{} must be non-scalar'.format(k))
                    bizobj_list = value
                    self._relationship_data[k] = bizobj_list
                elif is_sequence:
                    if not rel.many:
                        raise ValueError('{} must be scalar'.format(k))
                    self._relationship_data[k] = value

            def fdel(self):
                del self._relationship_data[k]

            return property(fget=fget, fset=fset, fdel=fdel)

        for rel in relationships.values():
            setattr(cls, rel.name, build_rel_property(rel.name, rel))


class BizObject(DirtyInterface, JsonPatchMixin, metaclass=BizObjectMeta):

    _schema = None  # set by metaclass
    _relationships = {}  # set by metaclass
    _dao_manager = DaoManager.get_instance()

    def __init__(self, data=None, **kwargs_data):
        super(BizObject, self).__init__()
        self._data = self._load(data, kwargs_data)
        self._public_id = None
        self._bizobj_id = None
        self._cached_dump_data = None

        if self._schema is not None:
            self._is_id_in_schema = '_id' in self._schema.fields
            self._is_public_id_in_schema = 'public_id' in self._schema.fields
        else:
            self._is_id_in_schema = False
            self._is_public_id_in_schema = False

    def __getitem__(self, key):
        if key in self._data:
            return self._data[key]
        elif key in self._relationships:
            return self._relationships[key].data
        elif self._schema and key in self._schema.fields:
            return None

        raise KeyError(key)

    def __setitem__(self, key, value):
        if key in self._relationships:
            rel = self._relationships[key]
            is_sequence = isinstance(value, (list, tuple, set))
            if rel.many:
                assert is_sequence
            else:
                assert not is_sequence
            rel.data = list(value)

        if self._schema is not None:
            if key not in self._schema.fields:
                raise KeyError('{} not in {} schema'.format(
                        key, self._schema.__class__.__name__))

        self._data[key] = value

    def __contains__(self, key):
        return key in self._data

    def __repr__(self):
        bizobj_id = ''
        _id = self._id
        if _id is not None:
            bizobj_id = '/id={}'.format(_id)
        else:
            public_id = self.public_id
            if public_id is not None:
                bizobj_id = '/public_id={}'.format(public_id)

        dirty_flag = '*' if self._data.dirty else ''

        return '<{class_name}{dirty_flag}{bizobj_id}>'.format(
                class_name=self.__class__.__name__,
                bizobj_id=bizobj_id,
                dirty_flag=dirty_flag)

    @classmethod
    @abstractmethod
    def __schema__(cls) -> str:
        """
        Return a dotted path to the Schema class, like 'path.to.MySchema'.
        """

    @classmethod
    @abstractmethod
    def __dao__(cls) -> str:
        """
        Return a dotted path to the DAO class, like 'path.to.MyDao'.
        """

    @classmethod
    def get_dao(cls):
        return cls._dao_manager.get_dao_for_bizobj(cls)

    @property
    def data(self):
        return self._data

    @property
    def relationships(self):
        return self._relationship_data

    @property
    def dao(self):
        return self.get_dao()

    def keys(self):
        return self._data.keys()

    def values(self):
        return self._data.values()

    def items(self):
        return self._data.items()

    def update(self, dct):
        self._cached_dump_data = None
        self._data.update(dct)
        self.mark_dirty(dct.keys())

    def dump(self):
        """
        Dump the fields of this business object along with its related objects
        (declared as relationships) to a plain ol' dict.
        """
        if self._cached_dump_data:
            data = self._cached_dump_data
        else:
            data = self._dump_schema()
            self._cached_dump_data = data
        related_data = self._dump_relationships()
        data.update(related_data)
        return data

    def _dump_schema(self):
        """
        Dump all scalar fields of the instance to a dict.
        """
        if self._schema is not None:
            return self._schema.dump(self._data).data
        return self._data.copy()

    def _dump_relationships(self):
        """
        If no schema is associated with the instance, we dump all relationship
        data that exists. Otherwise, we only dump data declared as corresponding
        fields in the schema.
        """
        data = {}
        for rel_name, rel_val in self.relationships.items():
            # rel_val is the actual object or list associated with the
            # relationship; whereas, just rel is the relationship object itself.
            rel = self._relationships[rel_name]
            load_from_field = rel.load_from or rel.name
            has_field = load_from_field in self._schema.fields
            if not self._schema or (load_from_field in self._schema.fields):
                dump_to = rel.dump_to or rel.name
                if is_bizobj(rel_val):
                    data[dump_to] = rel_val.dump()
                else:
                    assert isinstance(rel_val, (list, set, tuple))
                    data[dump_to] = [bizobj.dump() for bizobj in rel_val]
        return data

    def _load(self, data, kwargs_data):
        """
        Load data passed into the bizobj ctor into an internal DirtyDict. If any
        of the data fields correspond with delcared Relationships, load the
        bizobjs declared by said Relationships from said data.
        """
        data = data or {}
        data.update(kwargs_data)

        # NOTE: When bizobjs are passed into the ctor instead of raw dicts, the
        # bizobjs are not copied, which means that if some other bizobj also
        # references these bizobj and makes changes to them, the changes will
        # also have an effect here.

        # eagerly load all related bizobjs from the loaded data dict,
        # removing the fields from said dict.
        for rel in self._relationships.values():
            if rel.name in self._schema.fields:
                load_from = rel.load_from or rel.name
                related_data = data.pop(load_from, None)

                if related_data is None:
                    self._relationship_data[rel.name] = None
                    continue

                if rel.many:
                    related_bizobj_list = []
                    for obj in related_data:
                        if isinstance(obj, rel.bizobj_class):
                            related_bizobj_list.append(obj)
                        else:
                            related_bizobj_list.append(
                                rel.bizobj_class(related_data))

                    self._relationship_data[rel.name] = related_bizobj_list

                else:
                    if not is_bizobj(related_data):
                        # if the assertion below fails, then most likely
                        # you're intended to use the many=True kwarg in a
                        # relationship. The data coming in from load is a
                        # list, but without the 'many' kwarg set, the
                        # Relationship assumes that the 'related_data' is a
                        # dict and tries to call a bizobj ctor with it.
                        assert isinstance(related_data, dict)
                        related_bizobj = rel.bizobj_class(related_data)
                    else:
                        related_bizobj = related_data
                        self._relationship_data[rel.name] = related_bizobj

        if self._schema is not None:
            result = self._schema.load(data)
            if result.errors:
                raise Exception(str(result.errors))
            data = result.data

        # at this point, the data dict has been cleared of any fields that are
        # shadowed by Relationships declared on the bizobj class.
        return DirtyDict(data)

    @property
    def dirty(self):
        return self._data.dirty

    def set_parent(self, key_in_parent, parent):
        self._data.set_parent(key_in_parent, parent)

    def has_parent(self, obj):
        return self._data.has_parent(obj)

    def get_parent(self):
        return self._data.get_parent()

    def mark_dirty(self, key_or_keys):
        self._data.mark_dirty(key_or_keys)
        self._cached_dump_data = None

    def clear_dirty(self, keys=None):
        self._data.clear_dirty(keys=keys)

    def save(self, fetch=False):
        nested_bizobjs = []
        data_to_save = {}

        # depth-first save nested bizobjs.
        for k, v in self.relationships.items():
            if not v:
                continue
            rel = self._relationships[k]
            if rel.many:
                dumped_list_item_map = {}
                # TODO keep track in the rel of which bizobj are dirty
                # to avoid O(N) scan of list
                for i, bizobj in enumerate(v):
                    if bizobj.dirty:
                        bizobj.save(fetch=fetch)
                        dumped_list_item_map[i] = bizobj.dump()
                if dumped_list_item_map:
                    data_to_save[k] = dumped_list_item_map
            elif v.dirty:
                v.save()
                data_to_save[k] = v.dump()

        for k in self._data.dirty:
            data_to_save[k] = self[k]

        # persist data and refresh data
        if data_to_save:
            _id = self.dao.save(self._id, data_to_save)
            self._id = _id
            if fetch:
                self.update(self.dao.fetch(_id=_id))
            self.clear_dirty()
