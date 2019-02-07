import uuid

from copy import deepcopy, copy
from typing import List, Dict, Text, Type, Tuple, Set

from pybiz.dao.dao_binder import DaoBinder
from pybiz.dao.python_dao import PythonDao
from pybiz.util import is_bizobj, is_sequence, repr_biz_id
from pybiz.dirty import DirtyDict

from .meta import BizObjectMeta
from .dump import DumpNested, DumpSideLoaded
from .query import Query, QueryUtils


class BizObject(metaclass=BizObjectMeta):

    schema = None
    relationships = {}

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
        return PythonDao()

    @classmethod
    def get_dao(cls) -> 'Dao':
        """
        Get the global Dao reference associated with this class.
        """
        binder = DaoBinder.get_instance()
        return binder.get_dao_instance(cls, bind=True)

    def __init__(self, data=None, **more_data):
        self._data = DirtyDict()
        self._related = {}
        self._hash = int(uuid.uuid4().hex, 16)
        self.merge(dict(data or {}, **more_data))

    def __hash__(self):
        return self._hash

    def __eq__(self):
        return self._id == other._id if self._id is not None else False

    def __getitem__(self, key):
        if key in self.schema.fields or key in self.relationships:
            return getattr(self, key)
        raise KeyError(key)

    def __setitem__(self, key, value):
        if key in self.schema.fields or key in self.relationships:
            return setattr(self, key, value)
        raise KeyError(key)

    def __delitem__(self, key):
        if key in self.schema.fields or key in self.relationships:
            delattr(self, key)
        else:
            raise KeyError(key)

    def __iter__(self):
        return iter(self._data)

    def __contains__(self, key):
        return key in self._data

    def __repr__(self):
        id_str = repr_biz_id(self)
        name = self.__class__.__name__
        dirty = '*' if self._data.dirty else ''
        return f'<{name}({id_str}){dirty}>'

    @classmethod
    def exists(cls, _id=None) -> bool:
        """
        Does a simple check if a BizObject exists by id.
        """
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

        results = query.execute()
        if first:
            return results[0] if results else None
        else:
            return cls.BizList(results)

    @classmethod
    def get(cls, _id, fields: Dict = None) -> 'BizObject':
        _id, err = cls.schema.fields['_id'].process(_id)
        if err:
            raise Exception(err)  # TODO: raise validation error

        fields, children = QueryUtils.prepare_fields_argument(cls, fields)
        record = cls.get_dao().fetch(_id=_id, fields=fields)
        if record is not None:
            bizobj = cls(record)

            # recursively load nested relationships
            QueryUtils.query_relationships(bizobj, children)
            return bizobj.clean()

        return None

    @classmethod
    def get_many(cls, _ids = None, fields: List=None, as_list=True):
        _ids = _ids or []
        processed_ids = []
        for _id in _ids:
            processed_id, err = cls.schema.fields['_id'].process(_id)
            processed_ids.append(processed_id)
            if err:
                raise Exception(err)  # TODO: raise validation error

        # separate field names into those corresponding to this BizObjects
        # class and those of the related BizObject classes.
        fields, children = QueryUtils.prepare_fields_argument(cls, fields)

        # fetch data from the dao
        records = cls.get_dao().fetch_many(_ids=processed_ids, fields=fields)

        # now fetch and merge related business objects.
        # This could be optimized.
        bizobjs = {}
        for _id, record in records.items():
            if record is not None:
                bizobjs[_id] = bizobj = cls(record).clean()
                QueryUtils.query_relationships(bizobj, children)
            else:
                bizobjs[_id] = None

        # return results either as a list or a mapping from id to object
        if as_list:
            return cls.BizList(list(bizobjs.values()))
        else:
            return bizobjs

    @classmethod
    def get_all(cls, fields: Set[Text] = None) -> Dict:
        return {
            _id: cls(record).clean()
            for _id, record in cls.get_dao().fetch_all().items()
        }

    @classmethod
    def delete_many(cls, bizobjs) -> None:
        bizobj_ids = []
        for obj in bizobjs:
            obj.mark(obj.data.keys())
            bizobj_ids.append(obj._id)
            obj._id = None
        cls.get_dao().delete_many(bizobj_ids)

    def delete(self) -> 'BizObject':
        """
        Call delete on this object's dao and therefore mark all fields as dirty
        and delete its _id so that save now triggers Dao.create.
        """
        self.dao.delete(_id=self._id)
        self.mark(self.data.keys())
        self._id = None
        return self

    @classmethod
    def save_many(cls, bizobjs: List['BizObject']) -> List['BizObject']:
        """
        Save many BizObjects
        """
        records_to_create = []
        records_to_update = []

        for b in bizobjs:
            record = {k: self._data[k] for k in b.dirty}
            for k, default in self.defaults.items():
                field = self.schema.fields[k]
                if k not in data or (record[v] is None and not field.nullable):
                    if callable(default):
                        record[k] = default()
                    else:
                        record[k] = deepcopy(default)
            if b._id is None:
                # TODO: generate defaults into this
                records_to_create.append(record)
            else:
                records_to_update.append(record)

        created_records = cls.get_dao().create_many(records_to_create)
        created_bizobjs = [cls(x).clean() for x in created_records.values()]

        updated_records = cls.get_dao().update_many(records_to_update)
        updated_bizobjs = [cls(x).clean() for x in updated_records.values()]

        # TODO: recurse on relationships

        return (created_bizobjs + updated_records)

    def save(self, path: List['BizObject'] = None) -> 'BizObject':
        # TODO: allow fields kwarg to specify a subset of fields and
        # relationships to save instead of all changes.
        self.pre_save(path)

        data_to_save = {k: self[k] for k in self._data.dirty}
        path = path or []

        if self._id is None:
            for k, default in self.defaults.items():
                if k not in data_to_save:
                    if callable(default):
                        data_to_save[k] = default()
                    else:
                        data_to_save[k] = deepcopy(default)
            updated_data = self.dao.create(data_to_save)
        else:
            updated_data = self.dao.update(self._id, data_to_save)

        if updated_data:
            for k, v in updated_data.items():
                setattr(self, k, v)

        # Save dirty child BizObjects before saving this BizObject so that the
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

        self.clean()
        self.post_save(path)

        return self

    def pre_save(self, path: List['BizObject']):
        pass

    def post_save(self, path: List['BizObject']):
        pass

    @property
    def dao(self) -> 'Dao':
        return self.get_dao()

    @property
    def data(self) -> 'DirtyDict':
        return self._data

    @property
    def related(self) -> Dict:
        return self._related

    @property
    def dirty(self) -> Set[Text]:
        return self._data.dirty

    def clean(self, keys=None) -> 'BizObject':
        self._data.clear_dirty(keys)
        return self

    def mark(self, keys) -> 'BizObject':
        if not is_sequence(keys):
            keys = {keys}
        self._data.mark_dirty({k for k in keys if k in self.schema.fields})
        for k in keys:
            if k in self.related:
                del self.related[k]
        return self

    def copy(self, deep=False) -> 'BizObject':
        """
        Create a clone of this BizObject. Deep copy its fields but, by default.

        Args:
        - `deep`: If set, deep copy related BizObjects.
        """
        clone = self.__class__(deepcopy(self.data))

        # select the copy method to use for relationship-loaded data
        copy_related_value = deepcopy if deep else copy

        # copy related BizObjects
        for k, v in self.related.items():
            if not self.relationships[k].many:
                clone.related[k] = copy_related_value(v)
            else:
                clone.related[k] = [copy_related_value(i) for i in v]

        return clone.clean()

    def merge(self, obj, process=True, mark=True) -> 'BizObject':
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
            dirty_keys = obj._data.keys()
        else:
            dirty_keys = obj.keys()
            for k, v in obj.items():
                setattr(self, k, v)

        if process:
            # run the new data dict through the schema
            processed_data, error = self.schema.process(self._data)
            if error:
                # TODO: raise custom exception
                raise Exception(str(error))

            previous_dirty = set(self.dirty)
            self._data = DirtyDict(processed_data)
            self.clean()
        else:
            previous_dirty = set()

        # clear cached dump data because we now have different data :)
        # and mark all new keys as dirty.
        if mark:
            self.mark(dirty_keys | previous_dirty)

        return self

    def load(self, fields=None) -> 'BizObject':
        """
        Assuming _id is not None, this will load the rest of the BizObject's
        data.
        """
        if isinstance(fields, str):
            fields = {fields}
        fresh = self.get(_id=self._id, fields=fields)
        self.merge(fresh, mark=False)
        return self

    def has(self, key) -> bool:
        """
        This tells you if any data has been loaded into the given field or
        relationship. Better to use this than to do "if user.friends" unless you
        intend for the "friends" Relationship to execute its query as a
        side-effect.
        """
        return (key in self.data or key in self.related)

    def dump(self, fields=None, style='nested') -> Dict:
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
