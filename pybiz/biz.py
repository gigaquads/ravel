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

import sys
import copy
import inspect

import venusian

from abc import ABCMeta, abstractmethod
from typing import List, Dict
from importlib import import_module

from appyratus.decorators import memoized_property
from appyratus.schema import Schema, fields as schema_fields

from .web.patch import JsonPatchMixin
from .web.graphql import GraphQLObject, GraphQLEngine
from .dao.base import Dao, DaoManager
from .dirty import DirtyDict, DirtyInterface
from .predicate import Predicate, ConditionalPredicate, BooleanPredicate
from .util.bizobj_util import is_bizobj
from .exc import NotFound
from .constants import (
    IS_BIZOBJ_ANNOTATION,
    PRE_PATCH_ANNOTATION,
    POST_PATCH_ANNOTATION,
    PATCH_PATH_ANNOTATION,
    PATCH_ANNOTATION,
)

# TODO: keep track which bizobj are dirty in relationships to avoid O(N) scan
# during the dump operation.


class Relationship(object):
    def __init__(
        self,
        target,
        many=False,
        dump_to=None,
        load_from=None,
        query=None,
    ):
        self._target = target
        self.load_from = load_from
        self.dump_to = dump_to
        self.many = many
        self.name = None
        self.query = query

    @memoized_property
    def target(self):
        if callable(self._target):
            return self._target()
        else:
            return self._target

    def copy(self):
        return copy.deepcopy(self)


class ComparableProperty(property):
    def __init__(self, key, **kwargs):
        super().__init__(**kwargs)
        self._key = key

    def __eq__(self, other):
        return ConditionalPredicate(self._key, '=', other)

    def __ne__(self, other):
        return ConditionalPredicate(self._key, '!=', other)

    def __lt__(self, other):
        return ConditionalPredicate(self._key, '<', other)

    def __le__(self, other):
        return ConditionalPredicate(self._key, '<=', other)

    def __gt__(self, other):
        return ConditionalPredicate(self._key, '>', other)

    def __ge__(self, other):
        return ConditionalPredicate(self._key, '>=', other)

    def __ge__(self, other):
        return ConditionalPredicate(self._key, '>=', other)

    @property
    def key(self):
        return self._key

    @property
    def asc(self):
        return (self._key, +1)

    @property
    def desc(self):
        return (self._key, -1)


class RelationshipProperty(property):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)


class BizObjectMeta(ABCMeta):
    def __new__(cls, name, bases, dict_):
        new_class = ABCMeta.__new__(cls, name, bases, dict_)
        cls.add_is_bizobj_annotation(new_class)
        return new_class

    def __init__(cls, name, bases, dict_):
        ABCMeta.__init__(cls, name, bases, dict_)

        relationships = cls.build_relationships()
        schema_class = cls.build_schema_class(name)

        cls.graphql = GraphQLEngine(cls)

        cls.build_all_properties(schema_class, relationships)
        cls.register_JsonPatch_hooks(bases)
        cls.register_dao()

        def venusian_callback(scanner, name, bizobj_type):
            scanner.bizobj_classes[name] = bizobj_type

        venusian.attach(cls, venusian_callback, category='biz')

    def register_dao(cls):
        dao_class = cls.__dao__()
        if dao_class:
            cls.dao_manager.register(cls, dao_class)

    def build_schema_class(cls, name):
        """
        Builds cls.Schema from the fields declared on the business object. All
        business objects automatically inherit an _id field.
        """

        # We begin by building the `fields` dict that will become attributes
        # of our dynamic Schema class being created below.
        # Ensure each BizObject Schema class has an _id Field

        # use the schema class override if defined
        obj = cls.__schema__()
        if obj:
            if isinstance(obj, str):
                schema_class = cls.import_schema_class(obj)
            elif isinstance(obj, type) and issubclass(obj, Schema):
                schema_class = obj
            else:
                raise ValueError(str(obj))
        else:
            schema_class = None

        fields = copy.deepcopy(schema_class.fields) if schema_class else {}

        # "inherit" fields of parent BizObject.Schema
        inherited_schema_class = getattr(cls, 'Schema', None)
        if inherited_schema_class is not None:
            for k, v in inherited_schema_class.fields.items():
                fields.setdefault(k, copy.deepcopy(v))

        # collect and field declared on this BizObject class
        for k, v in inspect.getmembers(
            cls, predicate=lambda x: isinstance(x, schema_fields.Field)
        ):
            if k is 'schema':
                # XXX `schema` gets recognized by getmembers as a Field
                continue
            fields[k] = v

        # bless each bizobj with a mandatory _id field.
        if '_id' not in fields:
            fields['_id'] = schema_fields.Field(nullable=True)

        # Build string name of the new Schema class
        # and construct the Schema class object:
        cls.Schema = Schema.factory('{}Schema'.format(name), fields)
        cls.schema = cls.Schema()

        return cls.Schema

    def add_is_bizobj_annotation(new_class):
        """
        Set this attribute in order to be able to use duck typing to check
        isinstance of BizObjects.
        """
        setattr(new_class, IS_BIZOBJ_ANNOTATION, True)

    def build_all_properties(cls, schema_class, relationships):
        """
        The names of Fields declared in the Schema and Relationships declared on
        the BizObject will overwrite any methods or attributes defined
        explicitly on the BizObject class. This happens here.
        """
        cls.build_relationship_properties(relationships)
        cls.relationships = relationships
        cls.build_field_properties(cls.schema, relationships)

    def import_schema_class(self, class_path_str):
        class_path = class_path_str.split('.')
        assert len(class_path) > 1

        module_path_str = '.'.join(class_path[:-1])
        class_name = class_path[-1]

        try:
            schema_module = import_module(module_path_str)
            schema_class = getattr(schema_module, class_name)
        except Exception:
            raise ImportError(
                'failed to import schema class {}'.format(class_name)
            )

        return schema_class

    def register_JsonPatch_hooks(cls, bases):
        if not any(issubclass(x, JsonPatchMixin) for x in bases):
            return

        # scan class methods for those annotated as patch hooks
        # and register them as such.
        for k, v in inspect.getmembers(cls, predicate=inspect.ismethod):
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

        is_not_method = lambda x: not inspect.ismethod(x)
        for k, rel in inspect.getmembers(cls, predicate=is_not_method):
            is_relationship = isinstance(rel, Relationship)
            if not is_relationship:
                is_super_relationship = k in cls.relationships
                if is_super_relationship:
                    super_rel = cls.relationships[k]
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

            return ComparableProperty(k, fget=fget, fset=fset, fdel=fdel)

        for field_name in schema.fields:
            if field_name not in relationships:
                setattr(cls, field_name, build_property(field_name))

    def build_relationship_properties(cls, relationships):
        def build_relationship_property(k, rel):
            def fget(self):
                """
                Return the related BizObject instance or list.
                """
                DNE = '%{}%'.format(sys.maxsize)
                retval = self._related_bizobjs.get(k, DNE)
                if retval is DNE:
                    if rel.query is not None:
                        retval = rel.query(self)
                        setattr(self, k, retval)    # go through fset
                    elif rel.many:
                        retval = []
                    else:
                        retval = None
                return retval

            def fset(self, value):
                """
                Set the related BizObject or list, enuring that a list can't
                be assigned to a Relationship with many == False and vice
                versa..
                """
                rel = self.relationships[k]
                if isinstance(value, dict):
                    value = list(value.values())
                is_sequence = isinstance(value, (list, tuple, set))
                if not is_sequence:
                    if rel.many:
                        raise ValueError('{} must be non-scalar'.format(k))
                    bizobj_list = value
                    self._related_bizobjs[k] = bizobj_list
                elif is_sequence:
                    if not rel.many:
                        raise ValueError('{} must be scalar'.format(k))

                    self._related_bizobjs[k] = value

            def fdel(self):
                """
                Remove the related BizObject or list. The field will appeear in
                dump() results. You must assign None if you want to None to appear.
                """
                del self._related_bizobjs[k]

            return RelationshipProperty(fget=fget, fset=fset, fdel=fdel)

        for rel in relationships.values():
            setattr(cls, rel.name, build_relationship_property(rel.name, rel))


class BizObject(
    DirtyInterface,
    JsonPatchMixin,
    GraphQLObject,
    metaclass=BizObjectMeta
):
    dao_manager = DaoManager.get_instance()

    schema = None         # set by metaclass
    relationships = {}    # set by metaclass

    @classmethod
    def __schema__(cls):
        """
        Return a dotted path to the Schema class, like 'path.to.MySchema'. This
        is only used if a Schema subclass is returned. If None is returned,
        this is ignored.
        """

    @classmethod
    def __dao__(cls):
        """
        Returns a dotted path or Python reference to a Dao class to back this
        BizObject. Normally, this information should be declared in a manifest.
        """

    @classmethod
    def get_dao(cls):
        return cls.dao_manager.get_dao(cls)

    def __init__(self, data=None, **kwargs_data):
        JsonPatchMixin.__init__(self)
        DirtyInterface.__init__(self)
        GraphQLObject.__init__(self)
        self._related_bizobjs = {}
        self._cached_dump_data = None
        self._data = self._load(data, kwargs_data)

    def __getitem__(self, key):
        """
        If the key is the name of a field, return it. If it's a relationship,
        return the related BizObject or list of BizObjects. Otherwise, raise
        a KeyError.
        """
        if key in self._data:
            return self._data[key]
        elif key in self._related_bizobjs:
            return self._related_bizobjs[key]
        elif key in self.schema.fields:
            return None

        raise KeyError(key)

    def __setitem__(self, key, value):
        """
        Set the field or related BizObject on the instance, but if the key does
        not correspond to either a field or Relationship, raise KeyError.
        """
        # if the data being set is a BizObject associated through a declared
        # Relationship, add it to the "related_bizobj" storage dict.
        if key in self.relationships:
            rel = self.relationships[key]
            is_sequence = isinstance(value, (list, tuple, set))
            if rel.many:
                assert is_sequence
            else:
                assert not is_sequence
            self._related_bizobjs[key] = value
            return

        # disallow assignment of any field not declared in the Schema class
        # otherwise, set it on the data dict.
        if key in self.schema.fields:
            self._data[key] = value
        else:
            raise KeyError(
                '{} not in {} schema'.
                format(key, self.schema.__class__.__name__)
            )

    def __contains__(self, key):
        return key in self._data

    def __repr__(self):
        return '<{name}({id}){dirty}>'.format(
            id=self._data.get('_id') or '?',
            name=self.__class__.__name__,
            dirty='*' if self._data.dirty else '',
        )

    # -- CRUD Interface --------------------------------------------

    @classmethod
    def exists(cls, _id=None):
        return cls.get_dao().exists(_id=_id)

    @classmethod
    def query(
        cls,
        predicate: Predicate,
        fields: Dict=None,
        first: bool=False,
        **kwargs
    ):
        def load_predicate_values(pred):
            if isinstance(pred, ConditionalPredicate):
                field = cls.Schema.fields.get(pred.attr_name)
                if field is None:
                    # XXX Anything does not exist so this would break
                    field = Anything()
                pred.value = field.load(pred.value).value
            elif isinstance(pred, BooleanPredicate):
                load_predicate_values(pred.lhs)
                load_predicate_values(pred.rhs)
            return pred

        fields = cls._parse_fields(fields)
        records = cls.get_dao().query(
            predicate=load_predicate_values(predicate),
            fields=fields['self'],
            first=first,
            **kwargs
        )

        if first:
            retval = None
            if records:
                retval = cls(records[0]).clear_dirty()
                cls._query_relationships(retval, fields['related'])
        else:
            retval = []
            for record in records:
                bizobj = cls(record).clear_dirty()
                cls._query_relationships(bizobj, fields['related'])
                retval.append(bizobj)

        return retval

    @classmethod
    def get(cls, _id, fields: List=None):
        fields = cls._parse_fields(fields)
        record = cls.get_dao().fetch(_id=_id, fields=fields['self'])
        bizobj = cls(record).clear_dirty()

        if not (bizobj and bizobj._id):
            raise NotFound(_id)

        cls._query_relationships(bizobj, fields['related'])

        return bizobj

    @classmethod
    def get_many(cls, _ids, fields: List=None, as_list=False):
        # separate field names into those corresponding to this BizObjects
        # class and those of the related BizObject classes.
        fields = cls._parse_fields(fields)

        # fetch data from the dao
        records = cls.get_dao().fetch_many(_ids=_ids, fields=fields['self'])

        # now fetch and merge related business objects. This could be
        # optimized.
        bizobjs = {}
        for _id, record in records.items():
            bizobj = cls(record).clear_dirty()
            cls._query_relationships(bizobj, fields['related'])
            bizobjs[_id] = bizobj

        # return results either as a list or a mapping from id to object
        return bizobjs if not as_list else list(bizobjs.values())

    @classmethod
    def delete_many(cls, bizobjs):
        bizobj_ids = []
        for obj in bizobjs:
            obj.mark_dirty(obj.data.keys())
            bizobj_ids.append(obj._id)
        cls.get_dao().delete_many(bizobj_ids)

    def delete(self):
        self.dao.delete(_id=self._id)
        self.mark_dirty(self.data.keys())

    def save(self, fetch=False):
        nested_bizobjs = []
        data_to_save = {}

        # Save ditty child BizObjects before saving this BizObject so that the
        # updated child data can be passed into this object's dao.save/create
        # method
        for k, v in self._related_bizobjs.items():
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
                        dumped_list_item_map[i] = copy.deepcopy(bizobj.data)

                if dumped_list_item_map:
                    data_to_save[k] = dumped_list_item_map

            elif v.dirty:
                # this is a scalar relationship
                v.save(fetch=fetch)
                data_to_save[k] = copy.deepcopy(v.data)

        for k in self._data.dirty:
            data_to_save[k] = self[k]

        # Persist and refresh data
        if self._id is None:
            updated_data = self.dao.create(data_to_save)
        else:
            updated_data = self.dao.update(self._id, data_to_save)

        if updated_data:
            self.merge(updated_data)

        if fetch:
            self.merge(self.dao.fetch(_id=self._id))

        self.clear_dirty()

        return self

    @classmethod
    def _parse_fields(cls, fields: List):
        results = {'self': set(), 'related': {}}
        for k in (fields or cls.schema.fields.keys()):
            if isinstance(k, dict):
                rel_name, rel_fields = list(k.items())[0]
                if rel_name in cls.relationships:
                    results['related'][rel_name] = rel_fields
            elif k in cls.relationships:
                schema = cls.relationships[k].target.schema
                results['related'][k] = set(schema.fields.keys())
            else:
                results['self'].add(k)

        if not results['self']:
            results['self'] = set(cls.schema.fields.keys())
        else:
            results['self'].add('_id')

        return results

    @staticmethod
    def _query_relationships(bizobj, fields: Dict):
        for k, fields in fields.items():
            v = bizobj.relationships[k].query(bizobj, fields=fields)
            setattr(bizobj, k, v)

    # -- DirtyInterface --------------------------------------------

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
        return self

    # --------------------------------------------------------------

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

    def merge(self, obj):
        """
        Merge another dict or BizObject's data dict into the data dict of this
        BizObject. Not called "update" because that would be confused as the
        name of the CRUD method. "Update" int the CRUD sense is performed by
        the save method.
        """
        # create a deep copy of the data so as other BizObjects that also
        # merge in or possess this data don't mutate the data stored here.
        data = copy.deepcopy(obj if isinstance(obj, dict) else obj.data)
        self._data.update(data)

        # clear cached dump data because we now have different data :)
        # and mark all new keys as dirty.
        self._cached_dump_data = None
        self.mark_dirty(data.keys())

    def load(self, fields=None):
        """
        Assuming _id is not None, this will load the rest of the BizObject's
        data.
        """
        self.merge(self.get(_id=self._id, fields=fields))
        return self

    def dump(self, fields=True, relationships=True):
        """
        Dump the fields of this business object along with its related objects
        (declared as relationships) to a plain ol' dict.
        """
        # as an optimization, we memoize the return value of dump per instance.
        # first. This only applies to fields. Each BizObject associated through
        # a Relationship memoizes its own dump dict.
        data = {}

        if fields:
            if not self._cached_dump_data:
                data = self._dump_fields()
                self._cached_dump_data = data
            data = self._cached_dump_data

        # recursively dump related BizObjects and add to final dump dict
        if relationships:
            related_data = self._dump_relationships()
            data.update(related_data)

        return data

    def _dump_fields(self):
        """
        Dump all scalar fields of the instance to a dict.
        """
        data_copy = copy.deepcopy(self.data)
        data_copy['id'] = data_copy.pop('_id')
        return data_copy

    def _dump_relationships(self):
        """
        If no schema is associated with the instance, we dump all relationship
        data that exists. Otherwise, we only dump data declared as
        corresponding fields in the schema.
        """
        data = {}

        for rel_name, rel_val in self._related_bizobjs.items():
            # rel_val is the actual object or list associated with the
            # relationship; rel is the relationship object itself.
            rel = self.relationships[rel_name]
            dump_to = rel.dump_to or rel.name

            # if the related object is null, dump null; therwise, if it
            # is an object, dump it. if a list, dump the list.
            if is_bizobj(rel_val):
                data[dump_to] = rel_val.dump()
            elif rel.many:
                data[dump_to] = [bizobj.dump() for bizobj in rel_val]
            else:
                data[dump_to] = None

        return data

    def _load(self, data, kwargs_data):
        """
        Load data passed into the bizobj ctor into an internal DirtyDict. If
        any of the data fields correspond with delcared Relationships, load the
        bizobjs declared by said Relationships from said data.
        """
        data = data or {}
        data.update(kwargs_data)

        self._related_bizobjs = {}

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
                continue

            # we're loading a list of BizObjects
            if rel.many:
                related_bizobj_list = []
                for obj in related_data:
                    if isinstance(obj, rel.target):
                        related_bizobj_list.append(obj)
                    else:
                        related_bizobj_list.append(
                            rel.target(related_data)
                        )

                self._related_bizobjs[rel.name] = related_bizobj_list

            # We are loading a single BizObject. If obj is a plain dict,
            # we instantiate the related BizObject automatically.
            else:
                if not is_bizobj(related_data):
                    # if the assertion below fails, then most likely
                    # you're intended to use the many=True kwarg in a
                    # relationship. The data coming in from load is a
                    # list, but without the 'many' kwarg set, the
                    # Relationship assumes that the 'related_data' is a
                    # dict and tries to call a bizobj ctor with it.
                    assert isinstance(related_data, dict)
                    related_bizobj = rel.target(related_data)
                else:
                    related_bizobj = related_data
                    self._related_bizobjs[rel.name] = related_bizobj

        result, error = self.schema.process(data)
        if error:
            # TODO: raise custom exception
            raise Exception(str(error))

        # at this point, the data dict has been cleared of any fields that are
        # shadowed by Relationships declared on the bizobj class.
        return DirtyDict(result)
