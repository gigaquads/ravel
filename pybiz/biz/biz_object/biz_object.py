import uuid

import venusian

from copy import deepcopy, copy
from typing import List, Dict, Text, Type, Tuple, Set
from collections import defaultdict

from appyratus.utils import DictObject, DictUtils

from pybiz.dao.dao_binder import DaoBinder
from pybiz.dao.python_dao import PythonDao
from pybiz.util.misc_functions import (
    is_bizobj,
    is_sequence,
    repr_biz_id,
    normalize_to_tuple,
)
from pybiz.util.loggers import console
from pybiz.util.dirty import DirtyDict
from pybiz.exc import ValidationError, BizObjectError

from ..query import Query
from ..dump import NestingDumper, SideLoadingDumper
from ..biz_thing import BizThing
from .biz_object_meta import BizObjectTypeBuilder, BizObjectMeta


class BizObject(BizThing, metaclass=BizObjectMeta):

    Schema = None
    BizList = None

    schema = None
    relationships = {}

    is_bootstrapped = False
    is_abstract = False

    binder = DaoBinder.get_instance()  # TODO: Make into property
    api = None

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
        return PythonDao

    @classmethod
    def get_dao(cls) -> 'Dao':
        """
        Get the global Dao reference associated with this class.
        """
        return cls.binder.get_dao_instance(cls)

    @classmethod
    def select(cls, *select) -> 'Query':
        """
        Initialize and return a Query with cls as the target class.
        """
        if not keys:
            keys = tuple(cls.schema.fields.keys())
        return Query(cls).select(*select)

    @classmethod
    def generate(cls, fields: Set[Text] = None) -> 'BizObject':
        """
        Recursively generate a fixture for this BizObject class and any related
        objects as well.
        """
        field_names, children = set(), {}
        if fields:
            unflattened = DictUtils.unflatten_keys({k: None for k in fields})
            for k, v in unflattened.items():
                if v is None:
                    field_names.add(k)
                else:
                    children[k] = v

        data = cls.schema.generate(fields=field_names)
        for k, v in children.items():
            rel = cls.attributes.relationships.get(k)
            if rel:
                data[k] = rel.target_biz_type.generate(v)

        return cls(data=data)

    def __init__(self, data=None, **more_data):
        self.internal = DictObject({
            'hash': int(uuid.uuid4().hex, 16),
            'record': DirtyDict(),
            'memoized': {},
        })
        self.merge(dict(data or {}, **more_data))

    def __hash__(self):
        return self.internal.hash

    def __getitem__(self, key):
        if key in self.selectable_attribute_names:
            return getattr(self, key)
        raise KeyError(key)

    def __setitem__(self, key, value):
        if key in self.selectable_attribute_names:
            return setattr(self, key, value)
        raise KeyError(key)

    def __delitem__(self, key):
        if key in self.selectable_attribute_names:
            delattr(self, key)
        else:
            raise KeyError(key)

    def __iter__(self):
        return iter(self.internal.record)

    def __contains__(self, key):
        return key in self.internal.record

    def __repr__(self):
        id_str = repr_biz_id(self)
        name = self.__class__.__name__
        dirty = '*' if self.internal.record.dirty else ''
        return f'<{name}({id_str}){dirty}>'

    @classmethod
    def bootstrap(cls, api: 'Api', **kwargs):
        cls.api = api
        for biz_attr in cls.attributes.values():
            biz_attr.bootstrap(api)
        cls.on_bootstrap()
        cls.is_bootstrapped = True

    @classmethod
    def on_bootstrap(cls, **kwargs):
        pass

    @classmethod
    def bind(cls, binder: 'DaoBinder'):
        cls.binder = binder
        cls.on_bind()

    @classmethod
    def on_bind(cls):
        pass

    @classmethod
    def is_bound(cls):
        if cls.binder is not None:
            return cls.binder.is_bound(cls)
        return False

    @classmethod
    def exists(cls, obj=None) -> bool:
        """
        Does a simple check if a BizObject exists by id.
        """
        _id = obj._id if is_bizobj(obj) else obj
        if _id is not None:
            return cls.get_dao().exists(_id=_id)
        return False

    @classmethod
    def query(
        cls,
        select: Set[Text] = None,
        where: 'Predicate' = None,
        order_by: Tuple[Text] = None,
        offset: int = None,
        limit: int = None,
        first=False,
    ):
        """
        Alternate syntax for building Query objects manually.
        """
        query = Query.from_keys(
            cls, keys=(select or cls.schema.fields.keys())
        )

        if where:
            query.where(normalize_to_tuple(where))
        if order_by:
            query.order_by(normalize_to_tuple(order_by))
        if limit is not None:
            query.limit(limit)
        if offset is not None:
            query.offset(offset)

        return query.execute(first=first)

    @classmethod
    def get(cls, _id, select=None) -> 'BizObject':
        return cls.query(
            select=select,
            where=(cls._id == _id),
            first=True
        )

    @classmethod
    def get_many(
        cls,
        _ids: List = None,
        select=None,
        offset=None,
        limit=None,
        order_by=None,
    ) -> 'BizList':
        """
        Return a list or _id mapping of BizObjects.
        """
        return cls.query(
            select=select,
            where=cls._id.including(_ids),
            order_by=order_by,
            offset=offset,
            limit=limit,
        )

    @classmethod
    def get_all(
        cls,
        select: Set[Text] = None,
        offset: int = None,
        limit: int = None,
    ) -> 'BizList':
        return cls.query(
            select=select,
            where=cls._id != None,
            order_by=cls._id.asc,
            offset=offset,
            limit=limit,
        )

    def delete(self) -> 'BizObject':
        """
        Call delete on this object's dao and therefore mark all fields as dirty
        and delete its _id so that save now triggers Dao.create.
        """
        self.dao.delete(_id=self._id)
        self.mark(self.internal.record.keys())
        self._id = None
        return self

    @classmethod
    def delete_many(cls, bizobjs) -> None:
        bizobj_ids = []
        for obj in bizobjs:
            obj.mark(obj.internal.record.keys())
            bizobj_ids.append(obj._id)
            obj._id = None
        cls.get_dao().delete_many(bizobj_ids)

    @classmethod
    def delete_all(cls) -> None:
        cls.get_dao().delete_all()

    def create(self) -> 'BizObject':
        prepared_record = self.internal.record.copy()
        self.insert_defaults(prepared_record)
        prepared_record.pop('_rev', None)
        prepared_record, errors = self.schema.process(prepared_record)
        if errors:
            console.error(
                message=f'could not create {self.__class__.__name__} object',
                data=errors
            )
            raise ValidationError(
                message=f'could not create {self.__class__.__name__} object',
                data=errors
            )
        created_record = self.get_dao().create(prepared_record)
        self.internal.record.update(created_record)
        return self.clean()

    def update(self, data: Dict = None, **more_data) -> 'BizObject':
        data = dict(data or {}, **more_data)
        if data:
            self.merge(data)

        raw_record = self.dirty_data
        raw_record.pop('_rev', None)
        raw_record.pop('_id', None)

        errors = {}
        prepared_record = {}
        for k, v in raw_record.items():
            field = self.schema.fields.get(k)
            if field is not None:
                prepared_record[k], error = field.process(v)
                if error:
                    errors[k] = error

        # TODO: allow schema.process to take a subset of total keys
        if errors:
            raise ValidationError(
                message=f'could not update {self.__class__.__name__} object',
                data={
                    '_id': self._id,
                    'errors': errors,
                }
            )
        updated_record = self.get_dao().update(self._id, prepared_record)
        self.internal.record.update(updated_record)
        return self.clean()

    @classmethod
    def create_many(cls, bizobjs: List['BizObject']) -> 'BizList':
        """
        Call `dao.create_method` on input `BizObject` list and return them in
        the form of a BizList.
        """
        records = []

        for bizobj in bizobjs:
            if bizobj is None:
                continue
            record = bizobj.internal.record.copy()
            cls.insert_defaults(record)
            record, errors = cls.schema.process(record)
            record.pop('_rev', None)
            if errors:
                raise ValidationError(
                    message=(
                        f'could not create {cls.__name__} object: {errors}'
                    ),
                    data=errors
                )
            records.append(record)

        created_records = cls.get_dao().create_many(records)

        for bizobj, record in zip(bizobjs, created_records):
            bizobj.internal.record.update(record)
            bizobj.clean()

        return cls.BizList(bizobjs)

    @classmethod
    def update_many(
        cls,
        bizobjs: List['BizObject'],
        data: Dict = None,
        **more_data
    ) -> 'BizList':
        """
        Call the Dao's update_many method on the list of BizObjects. Multiple
        Dao calls may be made. As a preprocessing step, the input bizobj list
        is partitioned into groups, according to which subset of fields are
        dirty.

        For example, consider this list of bizobjs,

        ```python
        bizobjs = [
            user1,     # dirty == {'email'}
            user2,     # dirty == {'email', 'name'}
            user3,     # dirty == {'email'}
        ]
        ```

        Calling update on this list will result in two paritions:
        ```python
        assert part1 == {user1, user3}
        assert part2 == {user2}
        ```

        A spearate call to `dao.update_many` will be made for each partition.
        """
        # common_values are values that should be updated
        # across all objects.
        common_values = dict(data or {}, **more_data)

        # in the procedure below, we partition all incoming BizObjects
        # into groups, grouped by the set of fields being updated. In this way,
        # we issue an update_many statement for each partition in the DAL.
        partitions = defaultdict(list)

        for bizobj in bizobjs:
            if bizobj is None:
                continue
            if common_values:
                bizobj.merge(common_values)
            partitions[tuple(bizobj.dirty)].append(bizobj)

        for bizobj_partition in partitions.values():
            records, _ids = [], []

            for bizobj in bizobj_partition:
                record = bizobj.dirty_data
                record.pop('_rev', None)
                record.pop('_id', None)
                records.append(record)
                _ids.append(bizobj._id)

            console.debug(
                message='performing update_many',
                data={
                    'partition_size': len(_ids),
                    'total_size': len(bizobjs),
                }
            )
            updated_records = cls.get_dao().update_many(_ids, records)

            for bizobj, record in zip(bizobj_partition, updated_records):
                bizobj.internal.record.update(record)
                bizobj.clean()

        return cls.BizList(bizobjs)

    @classmethod
    def insert_defaults(cls, record: Dict) -> None:
        """
        This method is used internally and externally to insert field defaults
        into the `record` dict param.
        """
        generated_defaults = {}
        for k, default in cls.defaults.items():
            if k not in record:
                if callable(default):
                    defval = default()
                else:
                    defval = deepcopy(default)
                record[k] = defval
                generated_defaults[k] = defval

    @property
    def dao(self) -> 'Dao':
        return self.get_dao()

    @property
    def raw(self) -> 'DirtyDict':
        return self.internal.record

    @property
    def memoized(self) -> Dict:
        return self.internal.memoized

    @property
    def dirty_data(self) -> Dict:
        dirty_keys = self.dirty
        return {k: self.internal.record[k] for k in dirty_keys}

    @property
    def dirty(self) -> Set[Text]:
        return self.internal.record.dirty

    def clean(self, keys=None) -> 'BizObject':
        self.internal.record.clear_dirty(keys)
        return self

    def mark(self, keys) -> 'BizObject':
        if not is_sequence(keys):
            keys = {keys}
        self.internal.record.mark_dirty({k for k in keys if k in self.schema.fields})
        return self

    def copy(self, deep=False) -> 'BizObject':
        """
        Create a clone of this BizObject. Deep copy its fields but, by default.

        Args:
        - `deep`: If set, deep copy related BizObjects.
        """
        clone = self.__class__(deepcopy(self.internal.record))

        # select the copy method to use for relationship-loaded data
        copy_related_value = deepcopy if deep else copy

        # copy related BizObjects
        for k, v in self.related.items():
            if not self.relationships[k].many:
                clone.related[k] = copy_related_value(v)
            else:
                clone.related[k] = [copy_related_value(i) for i in v]

        return clone.clean()

    def merge(self, obj=None, **more_data) -> 'BizObject':
        """
        Merge another dict or BizObject's data dict into the data dict of this
        BizObject. Not called "update" because that would be confused as the
        name of the CRUD method.
        """
        if not (obj or more_data):
            return self

        if is_bizobj(obj):
            assert isinstance(obj, self.__class__)
            dirty_keys = obj.internal.record.keys()
            for k, v in obj.internal.record.items():
                setattr(self, k, v)
            for k, v in obj.internal.memoized.items():
                setattr(self, k, v)
        elif isinstance(obj, dict):
            obj = self.schema.translate_source(obj)
            dirty_keys = obj.keys()
            for k, v in obj.items():
                if k in self.schema.fields or k in self.attributes:
                    setattr(self, k, v)

        if more_data:
            self.merge(obj=more_data)
            more_data = self.schema.translate_source(obj)

        return self

    def load(self, select=None, depth=0) -> 'BizObject':
        """
        Assuming _id is not None, this will load the rest of the BizObject's
        data. By default, relationship data is not loaded unless explicitly
        requested.
        """
        if isinstance(select, str):
            select = {select}

        console.debug(message='loading', data={
            'class': self.__class__.__name__,
            'instance': self._id,
            'select': select
        })

        fresh = self.get(_id=self._id, select=select)  # TODO: depth=depth
        if fresh:
            self.merge(fresh)
            self.clean(fresh.internal.record.keys())

        return self

    def reload(self, keys=None) -> 'BizObject':
        if isinstance(keys, str):
            keys = {keys}
        keys = {k for k in keys if self.is_loaded(k)}
        return self.load(keys)

    def unload(self, keys: Set[Text]) -> 'BizObject':
        """
        Remove the given keys from field data and/or relationship data.
        """
        keys = {keys} if isinstance(keys, str) else keys
        console.debug(message='unloading', data={
            'class': self.__class__.__name__,
            'instance': self._id,
            'keys': keys
        })
        for k in keys:
            if k in self.internal.record:
                self.internal.record.pop(k, None)
            elif k in self.internal.memoized:
                self.internal.memoized.pop(k, None)

    def is_loaded(self, keys: Set[Text]) -> bool:
        """
        Are all given field and/or relationship values loaded?
        """
        keys = {keys} if isinstance(keys, str) else keys
        for k in keys:
            if not (k in self.internal.record or k in self.internal.memoized):
                return False
        return True

    def dump(self, fields=None, style='nested') -> Dict:
        """
        Dump the fields of this business object along with its related objects
        (declared as relationships) to a plain ol' dict.
        """
        if style == 'nested':
            dump = NestingDumper()
        elif style == 'side':
            dump = SideLoadingDumper()
        else:
            return None

        result = dump(target=self, fields=fields)
        return result
