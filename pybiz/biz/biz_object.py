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

from typing import List, Dict, Set, Text
from importlib import import_module
from collections import defaultdict

from appyratus.memoize import memoized_property
from appyratus.schema import Schema, fields as schema_fields
from appyratus.schema.fields import Field
from appyratus.utils import StringUtils, DictUtils

from pybiz.web.patch import JsonPatchMixin
from pybiz.web.graphql import GraphQLObject, GraphQLEngine
from pybiz.dao.base import Dao, DaoManager
from pybiz.dao.dict_dao import DictDao
from pybiz.dirty import DirtyDict, DirtyInterface
from pybiz.predicate import Predicate, ConditionalPredicate, BooleanPredicate
from pybiz.util import is_bizobj
from pybiz.exc import NotFound
from pybiz.constants import (
    IS_BIZOBJ_ANNOTATION,
    PRE_PATCH_ANNOTATION,
    POST_PATCH_ANNOTATION,
    PATCH_PATH_ANNOTATION,
    PATCH_ANNOTATION,
)

from .relationship import Relationship, RelationshipProperty
from .dump import NestingDumpMethod, SideLoadingDumpMethod
from .comparable_property import ComparableProperty
from .meta import BizObjectMeta

from threading import local


class Specification(dict):
    def __init__(
        self,
        fields: Set[Text] = None,
        relationships: Dict[Text, 'Specification'] = None,
        limit: int = None,
        offset: int = None,
    ):
        self['fields'] = set(fields or [])
        self['relationships'] = relationships or {}
        self['limit'] = max(1, limit) if limit is not None else None
        self['offset'] = max(0, offset) if offset is not None else None

    @property
    def fields(self):
        return self['fields']

    @fields.setter
    def fields(self, fields):
        self['fields'] = fields

    @property
    def relationships(self):
        return self['relationships']

    @relationships.setter
    def relationships(self, relationships):
        self['relationships'] = relationships

    @property
    def limit(self):
        return self['limit']

    @property
    def offset(self):
        return self['offset']


class BizObject(
    DirtyInterface,
    JsonPatchMixin,
    GraphQLObject,
    metaclass=BizObjectMeta
):
    _dao_manager = DaoManager.get_instance()

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
        return DictDao(type_name=cls.__name__)

    @classmethod
    def get_dao(cls):
        return cls._dao_manager.get_dao(cls)

    def __init__(self, data=None, **kwargs_data):
        JsonPatchMixin.__init__(self)
        DirtyInterface.__init__(self)
        GraphQLObject.__init__(self)

        self._related = {}
        self._data = DirtyDict(self._load(data, kwargs_data))

    def __getitem__(self, key):
        """
        If the key is the name of a field, return it. If it's a relationship,
        return the related BizObject or list of BizObjects. Otherwise, raise
        a KeyError.
        """
        if key in self._data:
            return self._data[key]
        elif key in self._related:
            return self._related[key]
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
                if isinstance(data, dict):
                    # we assume here that the data is in the form: {_id: obj}
                    data = [
                        x if is_bizobj(x) else rel.target(x)
                        for x in value.values()
                    ]
                else:
                    # assume data is a list, tuple, set
                    data = [
                        x if is_bizobj(x) else rel.target(x)
                        for x in value
                    ]
            else:
                assert not is_sequence
                data = x if is_bizobj(x) else rel.target(x)

            self._related[key] = data

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
        predicate: Predicate = None,
        fields: Dict=None,
        first: bool=False,
        **kwargs
    ):
        def load_predicate_values(pred):
            if pred is None:
                return None
            elif isinstance(pred, ConditionalPredicate):
                field = cls.Schema.fields.get(pred.attr_name)
                if field is None:
                    field = Field()
                if not isinstance(pred.value, (list, tuple, set)):
                    load_res, load_err = field.process(pred.value)
                    if load_err:
                        raise Exception('invalid value')
                else:
                    load_res = []
                    for v in pred.value:
                        res, load_err = field.process(v)
                        if load_err:
                            raise Exception('invalid value')
                        else:
                            load_res.append(res)
                pred.value = load_res
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
    def query(
        cls,
        predicate: Predicate = None,
        specification: Specification = None,
        fields: Set[Text] = None,
        first=False,
        **kwargs
    ):
        if specification is None:
            specification = Specification()
        elif isinstance(specification, dict):
            specification = Specification(**specification)

        if not specification.fields:
            specification.fields |= cls.schema.fields.keys()

        specification.fields.add('_id')

        if fields is not None:
            fields = fields if isinstance(fields, set) else set(fields)
            specification.fields |= fields

        records = cls.get_dao().query(
            predicate=predicate,
            fields=specification.fields,
            limit=specification.limit,
            offset=specification.offset,
            first=first,
        )

        def recurse(bizobj, spec):
            for k, rel in bizobj.relationships.items():
                related_spec = spec.relationships.get(k)
                if related_spec is None:
                    continue
                if related_spec is True:
                    related_spec = Specification()
                elif isinstance(specification, dict):
                    related_spec = Specification(**related_spec)

                if not related_spec.fields:
                    related_spec.fields |= rel.target.schema.fields.keys()

                related_spec.fields.add('_id')

                if related_spec is not None:
                    related = rel.query(bizobj, related_spec)
                    setattr(bizobj, k, related)
                    if is_bizobj(related):
                        related = [related]
                    for related_bizobj in related:
                        recurse(related_bizobj, related_spec)

        bizobjs = []

        for record in records:
            bizobj = cls(record).clear_dirty()
            recurse(bizobj, specification)
            bizobjs.append(bizobj)

        if first:
            return bizobjs[0] if bizobjs else None
        else:
            return bizobjs


    @classmethod
    def get(cls, _id, fields: Dict = None):
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
        records = cls.get_dao().fetch_many(
            _ids=remaining_ids, fields=fields['self'])

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

    def pre_save(self, path: List['BizObject']):
        pass

    def post_save(self, path: List['BizObject']):
        pass

    def save(self, path: List['BizObject'] = None):
        self.pre_save(path)

        data_to_save = {k: self[k] for k in self._data.dirty}
        path = path or []

        if self._id is None:
            updated_data = self.dao.create(data_to_save)
        else:
            updated_data = self.dao.update(self._id, data_to_save)

        if updated_data:
            for k, v in updated_data.items():
                setattr(self, k, v)

        # Save ditty child BizObjects before saving this BizObject so that the
        # updated child data can be passed into this object's dao.save/create
        # method
        for k, v in self._related.items():
            # `k` is the declared name of the relationship
            # `v` is the related bizobj or list of bizbojs
            if not v:
                continue
            rel = self.relationships[k]
            if rel.many:
                # this is a non-scalar relationship
                for i, bizobj in enumerate(v):
                    if bizobj.dirty:
                        bizobj.save(path=path+[self])
            elif v.dirty:
                # this is a scalar relationship
                v.save(path=path + [self])

        self.clear_dirty()
        self.post_save(path)

        return self

    @classmethod
    def _parse_fields(cls, fields):
        if isinstance(fields, (list, tuple, set)):
            fields = {k: True for k in fields}

        fields = DictUtils.unflatten_keys(fields or {})
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

    def clear_dirty(self, keys=None):
        self._data.clear_dirty(keys=keys)
        return self

    # --------------------------------------------------------------

    @property
    def data(self):
        return self._data

    @property
    def related(self) -> Dict:
        return self._related

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
        source_data = obj.data if is_bizobj(obj) else obj
        self.data.update(self._load(source_data, {}))

        # clear cached dump data because we now have different data :)
        # and mark all new keys as dirty.
        self.mark_dirty(source_data.keys())

    def load(self, fields=None):
        """
        Assuming _id is not None, this will load the rest of the BizObject's
        data.
        """
        self.merge(self.get(_id=self._id, fields=fields))
        return self

    def dump(self, depth=0, fields=None, style='nested'):
        """
        Dump the fields of this business object along with its related objects
        (declared as relationships) to a plain ol' dict.
        """
        if style == 'nested':
            dumper = NestingDumpMethod()
        elif style == 'side':
            dumper = SideLoadingDumpMethod()
        else:
            return None

        return dumper.dump(self, depth=depth, fields=fields)

    def _load(self, data, kwargs_data):
        """
        Load data passed into the bizobj ctor into an internal DirtyDict. If
        any of the data fields correspond with delcared Relationships, load the
        bizobjs declared by said Relationships from said data.
        """
        data = data or {}
        data.update(kwargs_data)

        self._related = {}

        # NOTE: When bizobjs are passed into the ctor instead of raw dicts, the
        # bizobjs are not copied, which means that if some other bizobj also
        # references these bizobj and makes changes to them, the changes will
        # also have an effect here.

        # eagerly load all related bizobjs from the loaded data dict,
        # removing the fields from said dict.
        for rel in self.relationships.values():
            source = rel.source or rel.name
            related_data = data.pop(source, None)

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
                            rel.target(obj)
                        )

                self._related[rel.name] = related_bizobj_list

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
                self._related[rel.name] = related_bizobj

        result, error = self.schema.process(data)
        if error:
            # TODO: raise custom exception
            raise Exception(str(error))

        # at this point, the data dict has been cleared of any fields that are
        # shadowed by Relationships declared on the bizobj class.
        return result
