"""
Relationship Load/Dump Mechanics:
    Load:
        - If any relationship name matches a schema field name
          of type <List> or <Object>, try to load the raw data
          into relationship data.
    Dump:
        - Simply dump the related objects into the data dict
          returned from the schema dump.

"""

import os
import copy
import uuid
import re

import venusian

from abc import ABCMeta, abstractmethod
from types import MethodType
from importlib import import_module

from .patch import JsonPatchMixin
from .dao import DaoManager, Dao
from .dirty import DirtyDict, DirtyInterface
from .util import is_bizobj
from .schema import AbstractSchema, Schema, Field, Anything
from .id_generator import IdGenerator, UuidGenerator
from .const import (
    PRE_PATCH_ANNOTATION,
    POST_PATCH_ANNOTATION,
    PATCH_PATH_ANNOTATION,
    PATCH_ANNOTATION,
    IS_BIZOBJ_ANNOTATION,
    )


# TODO: keep track which bizobj are dirty in relationships to avoid O(N) scan
# during the dump operation.


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

    def __new__(cls, name, bases, dict_):
        new_class = ABCMeta.__new__(cls, name, bases, dict_)
        cls.add_is_bizobj_annotation(new_class)
        return new_class

    def __init__(cls, name, bases, dict_):
        ABCMeta.__init__(cls, name, bases, dict_)

        relationships = cls.build_relationships()
        schema_class = cls.build_schema_class(name)

        cls.build_all_properties(schema_class, relationships)
        cls.register_JsonPatch_hooks(bases)
        cls.register_dao()

        def callback(scanner, name, bizobj_class):
            scanner.bizobj_classes[name] = bizobj_class

        venusian.attach(cls, callback, category='biz')

    def register_dao(cls):
        dao_class = cls.__dao__()
        if dao_class:
            cls._dao_manager.register(cls, dao_class)

    def build_schema_class(cls, name):
        """
        Builds cls.Schema from the fields declared on the business object. All
        business objects automatically inherit an _id and public_id fields
        """
        schema_class_name = '{}Schema'.format(name)
        fields = dict(
            _id=Anything(load_only=True),
            public_id=Anything(dump_to='id', load_from='id'),
            )

        for k in dir(cls):
            v = getattr(cls, k)
            if isinstance(v, Field):
                fields[k] = v

        cls.Schema = type(schema_class_name, (Schema,), fields)
        return cls.Schema

    def add_is_bizobj_annotation(new_class):
        # set this attribute in order to be able to
        # use duck typing to check isinstance of BizObjects
        setattr(new_class, IS_BIZOBJ_ANNOTATION, True)

    def build_all_properties(cls, schema_class, relationships):
        # the names of Fields declared in the Schema and Relationships
        # declared on the BizObject will overwrite any methods or attributes
        # defined explicitly on the BizObject class. This happens here.
        cls.build_relationship_properties(relationships)
        cls.relationships = relationships

        # use the schema class override if defined
        schema_class_override = cls.__schema__()
        if schema_class_override:
            if isinstance(schema_class_override, str):
                schema_class = cls.import_schema_class(schema_class_override)
            else:
                schema_class = schema_class_override

        cls._schema = schema_class()
        cls._schema.strict = True

        cls.build_field_properties(cls._schema, relationships)

    def import_schema_class(self, class_path_str):
        class_path = class_path_str.split('.')
        assert len(class_path) > 1

        module_path_str = '.'.join(class_path[:-1])
        class_name = class_path[-1]

        try:
            schema_module = import_module(module_path_str)
            schema_class = getattr(schema_module, class_name)
        except:
            raise ImportError(
                'failed to import schema class {}'.format(class_name))

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
                return self._adjacent_bizobjs.get(k)

            def fset(self, value):
                rel = self.relationships[k]
                is_sequence = isinstance(value, (list, tuple, set))

                if not is_sequence:
                    if rel.many:
                        raise ValueError('{} must be non-scalar'.format(k))

                    bizobj_list = value
                    self._adjacent_bizobjs[k] = bizobj_list

                elif is_sequence:
                    if not rel.many:
                        raise ValueError('{} must be scalar'.format(k))

                    self._adjacent_bizobjs[k] = value

            def fdel(self):
                del self._adjacent_bizobjs[k]

            return property(fget=fget, fset=fset, fdel=fdel)

        for rel in relationships.values():
            setattr(cls, rel.name, build_rel_property(rel.name, rel))


class BizObjectCrudMethods(object):
    """
    BizObjectCrudMethods endows the BizObject with general boilerplate CRUD
    methods, which interface with the DAO. Note that the `save` method is
    actually an upsert.
    """

    @classmethod
    def get(cls, _id=None, public_id=None, fields: dict = None):
        return cls.get_dao().fetch(
            _id=_id, public_id=public_id, fields=fields)

    @classmethod
    def get_many(cls, _ids=None, public_ids=None, fields: dict = None):
        return cls.get_dao().fetch(
            _ids=_ids, public_ids=public_ids, fields=fields)

    @classmethod
    def delete_many(cls, bizobjs):
        cls.get_dao().delete_many([obj._id for obj in bizobjs])

    def delete(self):
        self.dao.delete(_id=self._id, public_id=self.public_id)

    def save(self, fetch=False):
        nested_bizobjs = []
        data_to_save = {}

        # Save ditty child BizObjects before saving this BizObject so that the
        # updated child data can be passed into this object's dao.save/create
        # method
        for k, v in self._adjacent_bizobjs.items():
            # `k` is the declared name of the relationship
            # `v` is the related bizobj or list of bizbojs
            if not v:
                continue

            rel = self.relationships[k]

            if rel.many:
                # this is a non-scalar relationship
                dumped_list_item_map = {}
                for i, bizobj in enumerate(v):
                    if bizobj.dirty:
                        bizobj.save(fetch=fetch)
                        dumped_list_item_map[i] = bizobj.dump()

                if dumped_list_item_map:
                    data_to_save[k] = dumped_list_item_map

            elif v.dirty:
                # this is a scalar relationship
                v.save()
                data_to_save[k] = v.dump()
                if '_id' in v.data:
                    data_to_save[k]['_id'] = v.data['_id']

        for k in self._data.dirty:
            data_to_save[k] = self[k]

        # Persist and refresh data
        if self._id is None:
            self._id = self._id_generator.next_id()

            if not self.public_id:
                self.public_id = self._id_generator.next_public_id()

            updated_data = self.dao.create(
                    _id=self._id,
                    public_id=self.public_id,
                    data=data_to_save)

        else:
            updated_data = self.dao.update(
                    _id=self._id,
                    public_id=self.public_id,
                    data=data_to_save)

        if updated_data:
            self.merge(updated_data)

        if fetch:
            self.merge(
                self.dao.fetch(
                    _id=self._id,
                    public_id=self.public_id
                ))

        self.clear_dirty()
        return self


class BizObjectDirtyDict(DirtyInterface):
    """
    BizObjectDirtyDict implements the DirtyInterface, meaning that it has the
    facility to keep track of what fields in its internal data dictionary have
    been modified as well as the ability to notify its parent dictionary of said
    changes, provided it has a parent.
    """

    def __init__(self, data, kwargs_data):
        DirtyInterface.__init__(self)
        self._data = self._load(data, kwargs_data)

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


class BizObjectSchema(AbstractSchema):
    """
    BizObjectSchema makes the BizObject into a Schema subclass, which the
    BizObjectMeta class uses to build a separate Schema from the fields declared
    directly on said BizObject. See the metaclass for implementation details.
    """


class BizObjectJsonPatch(JsonPatchMixin):
    """
    BizObjectJsonPatch provides the components necessary for implementing JSON
    Patch operations on the BizObject.
    """


class BizObject(
        BizObjectSchema,
        BizObjectJsonPatch,
        BizObjectCrudMethods,
        BizObjectDirtyDict,
        metaclass=BizObjectMeta
        ):

    _schema = None      # set by metaclass
    _relationships = {} # set by metaclass
    _dao_manager = DaoManager.get_instance()
    _id_generator = UuidGenerator()

    @classmethod
    def __schema__(cls):
        """
        Return a dotted path to the Schema class, like 'path.to.MySchema'. This
        is only used if a Schema subclass is returned. If None is returned, this
        is ignored.
        """

    @classmethod
    def __dao__(cls):
        """
        Returns a dotted path or Python reference to a Dao class to back this
        BizObject. Normally, this information should be declared in a manifest.
        """

    @classmethod
    def get_dao(cls):
        return cls._dao_manager.get_dao(cls)

    @classmethod
    def get_id_generator(cls):
        return cls._id_generator

    @classmethod
    def set_id_generator(cls, id_generator:IdGenerator):
        cls._id_generator = id_generator

    def __init__(self, data=None, **kwargs_data):
        self._adjacent_bizobjs = {}
        self._cached_dump_data = None

        # the order of these super class constructors matters...
        BizObjectJsonPatch.__init__(self)
        BizObjectCrudMethods.__init__(self)
        BizObjectSchema.__init__(self, strict=True, allow_additional=False)
        BizObjectDirtyDict.__init__(self, data, kwargs_data)

    def __getitem__(self, key):
        if key in self._data:
            return self._data[key]
        elif key in self.relationships:
            return self.relationships[key].data
        elif self._schema and key in self._schema.fields:
            return None

        raise KeyError(key)

    def __setitem__(self, key, value):
        if key in self.relationships:
            rel = self.relationships[key]
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

    @property
    def data(self):
        return self._data

    @property
    def dao(self):
        return self.get_dao()

    def keys(self):
        return self._data.keys()

    def values(self):
        return self._data.values()

    def items(self):
        return self._data.items()

    def merge(self, dct):
        """
        """
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

        for rel_name, rel_val in self._adjacent_bizobjs.items():
            # rel_val is the actual object or list associated with the
            # relationship; whereas, just rel is the relationship object itself.
            rel = self.relationships[rel_name]
            dump_to = rel.dump_to or rel.name
            if rel_val is None:
                data[dump_to] = None
            elif is_bizobj(rel_val):
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
        for rel in self.relationships.values():
            load_from = rel.load_from or rel.name
            related_data = data.pop(load_from, None)

            if related_data is None:
                #self._adjacent_bizobjs[rel.name] = None
                continue

            if rel.many:
                related_bizobj_list = []
                for obj in related_data:
                    if isinstance(obj, rel.bizobj_class):
                        related_bizobj_list.append(obj)
                    else:
                        related_bizobj_list.append(
                            rel.bizobj_class(related_data))

                self._adjacent_bizobjs[rel.name] = related_bizobj_list

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
                    self._adjacent_bizobjs[rel.name] = related_bizobj

        if self._schema is not None:
            result = self._schema.load(data)
            if result.errors:
                raise Exception(str(result.errors))
            data = result.data

        # at this point, the data dict has been cleared of any fields that are
        # shadowed by Relationships declared on the bizobj class.
        return DirtyDict(data)
