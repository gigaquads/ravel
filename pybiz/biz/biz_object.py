from typing import List, Dict, Text, Type, Tuple

from pybiz.web.patch import JsonPatchMixin
from pybiz.web.graphql import GraphQLObject
from pybiz.dao.base import DaoManager
from pybiz.dao.dict_dao import DictDao
from pybiz.dirty import DirtyDict, DirtyInterface
from pybiz.util import is_bizobj

from .meta import BizObjectMeta
from .dump import DumpNested, DumpSideLoaded
from .query import Query, QueryUtils


class BizObject(
    DirtyInterface, JsonPatchMixin, GraphQLObject,
    metaclass=BizObjectMeta
):
    """
    `BizObject` has built-in support for implementing GraphQL and REST API's.
    """
    schema = None         # set by metaclass
    relationships = {}    # set by metaclass

    @classmethod
    def __schema__(cls) -> Type['Schema']:
        """
        Declare the schema type/instance used by this BizObject class.
        """

    @classmethod
    def __dao__(cls) -> Type['Dao']:
        """
        Declare the DAO type/instance used by this BizObject class.
        """
        return DictDao(type_name=cls.__name__)

    @classmethod
    def get_dao(cls) -> 'Dao':
        """
        Get the global Dao reference associated with this class.
        """
        return DaoManager.get_instance().get_dao(cls)

    def __init__(self, data=None, **more_data):
        JsonPatchMixin.__init__(self)
        DirtyInterface.__init__(self)
        GraphQLObject.__init__(self)

        self._related = {}
        self._data = DirtyDict()

        # the metaclass has by now blessed this class Field and Relationship
        # properties. Go ahead and merge in input data by setting said
        # properties.
        self.merge(dict(data or {}, **more_data))

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
        predicate: 'Predicate' = None,
        specification: 'QuerySpecification' = None,
        order_by: Tuple[Text] = None,
        first=False,
    ) -> List['BizObject']:
        """
        Request a data structure containing the specified fields of this
        `BizObject` and all related `BizObject` instances declared with
        `Relationship`.

        The `specification` argument can be either,

        1. A well-formed `QuerySpecification` object,
        2. Nested dict, like `{'foo': {'bar': {'baz': None}}}`
        3. Set of dotted paths, like `{'foo', 'bar.baz'}`
        """
        query = Query(cls, predicate, specification)
        if order_by:
            query.spec.order_by = order_by

        bizobjs = query.execute()
        if first:
            return bizobjs[0].clear_dirty() if bizobjs else None
        else:
            return [obj.clear_dirty() for obj in bizobjs]

    @classmethod
    def get(cls, _id, fields: Dict = None):
        fields, children = QueryUtils.prepare_fields_argument(cls, fields)
        record = cls.get_dao().fetch(_id=_id, fields=fields)
        bizobj = cls(record)

        # recursively load nested relationships
        QueryUtils.query_relationships(bizobj, children)

        return bizobj.clear_dirty()

    @classmethod
    def get_many(cls, _ids, fields: List=None, as_list=True):
        # separate field names into those corresponding to this BizObjects
        # class and those of the related BizObject classes.
        fields, children = QueryUtils.prepare_fields_argument(cls, fields)

        # fetch data from the dao
        records = cls.get_dao().fetch_many(_ids=_ids, fields=fields)

        # now fetch and merge related business objects. This could be
        # optimized.
        bizobjs = {}
        for _id, record in records.items():
            bizobjs[_id] = bizobj = cls(record)
            QueryUtils.query_relationships(bizobj, children)
            bizobjs[_id].clear_dirty()

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
        # TODO: allow fields kwarg to specify a subset of fields and
        # relationships to save instead of all changes.
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
        if is_bizobj(obj):
            self._data.update(obj._data)
            self._related.update(obj._related)
        else:
            for k, v in obj.items():
                setattr(self, k, v)

        processed_data, error = self.schema.process(self._data)
        if error:
            # TODO: raise custom exception
            raise Exception(str(error))

        self._data = DirtyDict(processed_data)

        # clear cached dump data because we now have different data :)
        # and mark all new keys as dirty.
        self.mark_dirty({k for k in obj.keys() if k in self.schema.fields})

        return self

    def load(self, fields=None):
        """
        Assuming _id is not None, this will load the rest of the BizObject's
        data.
        """
        self.merge(self.get(_id=self._id, fields=fields))
        return self.clear_dirty(keys=fields)

    def is_loaded(self, fields):
        results = {}
        for k in fields:
            results[k] = k in self.data or k in self.related
        return results

    def dump(self, fields=None, style='nested'):
        """
        Dump the fields of this business object along with its related objects
        (declared as relationships) to a plain ol' dict.
        """
        if style == 'nested':
            dump = DumpNested()
        elif style == 'side':
            dump = DumpSideLoaded()
        else:
            return None

        result = dump(target=self, fields=fields)
        return result

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
            related_data = data.pop(rel.name, None)

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
