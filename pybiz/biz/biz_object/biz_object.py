import uuid

import venusian

from copy import deepcopy, copy
from typing import List, Dict, Text, Type, Tuple, Set
from collections import defaultdict

from appyratus.utils import DictObject, DictUtils

from pybiz.dao.python_dao import PythonDao
from pybiz.util.misc_functions import (
    is_bizobj,
    is_sequence,
    repr_biz_id,
    normalize_to_tuple,
)
from pybiz.util.loggers import console
from pybiz.util.dirty import DirtyDict
from pybiz.exceptions import ValidationError, BizObjectError

from .biz_object_meta import BizObjectTypeBuilder, BizObjectMeta
from ..dump import NestingDumper, SideLoadingDumper
from ..biz_thing import BizThing
from ..query import Query


class BizObject(BizThing, metaclass=BizObjectMeta):

    Schema = None
    BizList = None

    schema = None
    relationships = {}

    is_bootstrapped = False
    is_abstract = False

    app = None

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
    def get_dao(cls, bind=True) -> 'Dao':
        """
        Get the global Dao reference associated with this class.
        """
        binder = None
        if cls.app is not None:
            binder = cls.app.binder.get_dao_instance(cls, bind=bind)
        return binder

    @classmethod
    def select(cls, *selectors) -> 'Query':
        """
        Initialize and return a Query with cls as the target class.
        """
        if not selectors:
            selectors = tuple(cls.schema.fields.keys())
        return Query(cls).select(*selectors)

    @classmethod
    def generate(
        cls,
        fields: Set[Text] = None,
        constraints: Dict = None,
    ) -> 'BizObject':
        """
        Recursively generate a fixture for this BizObject class and any related
        objects as well.
        """
        field_names, children = set(), {}
        if not fields:
            fields = cls.schema.fields.keys()

        unflattened = DictUtils.unflatten_keys({k: None for k in fields})
        for k, v in unflattened.items():
            if k == '*':
                field_names |= cls.schema.fields.keys()
            elif k in cls.schema.fields:
                field_names.add(k)
            elif k in cls.attributes:
                attr = cls.attributes.by_name(k)
                if attr.category == 'relationship':
                    children[k] = v

        data = cls.schema.generate(
            fields=field_names,
            constraints=constraints
        )

        for k, v in children.items():
            attr = cls.attributes.by_name(k)
            if attr.category == 'relationship':
                # TODO: support nested constraints for BizAttributes
                data[k] = attr.target_biz_class.generate(v)

        return cls(data=data)

    def __init__(self, data=None, **more_data):
        data = data or {}
        if data.get('_id') is not None:
            hash_str = ''.join(
                hex(ord(c))[2:] for c in (
                    self.__class__.__name__.upper() + ':' + str(data['_id'])
                )
            )
        else:
            hash_str = uuid.uuid4().hex
        hash_int = int(hash_str, 16)
        self.internal = DictObject({
            'hash': hash_int,
            'arg': None,
            'state': DirtyDict(),
            'attributes': {},
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
        return iter(self.internal.state)

    def __contains__(self, key):
        return key in self.internal.state

    def __repr__(self):
        id_str = repr_biz_id(self)
        name = self.__class__.__name__
        dirty = '*' if self.internal.state.dirty else ''
        return f'<{name}({id_str}){dirty}>'

    @classmethod
    def bootstrap(cls, app: 'Application', **kwargs):
        cls.app = app
        for biz_attr in cls.attributes.values():
            biz_attr.bootstrap(app)
        cls.on_bootstrap()
        cls.is_bootstrapped = True

    @classmethod
    def on_bootstrap(cls, **kwargs):
        pass

    @classmethod
    def bind(cls, binder: 'ApplicationDaoBinder'):
        cls.binder = binder
        cls.on_bind()

    @classmethod
    def on_bind(cls):
        pass

    @classmethod
    def is_bound(cls):
        if cls.app and cls.app.binder:
            return cls.app.binder.is_bound(cls)
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
        execute=True,
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

        if not execute:
            return query
        else:
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
        Return a list of BizObjects in the store.
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
        """
        Return a list of all BizObjects in the store.
        """
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
        self.mark(self.internal.state.keys())
        self._id = None
        return self

    @classmethod
    def delete_many(cls, bizobjs) -> None:
        bizobj_ids = []
        for obj in bizobjs:
            obj.mark(obj.internal.state.keys())
            bizobj_ids.append(obj._id)
            obj._id = None
        cls.get_dao().delete_many(bizobj_ids)

    @classmethod
    def delete_all(cls) -> None:
        cls.get_dao().delete_all()

    def create(self, data: Dict = None) -> 'BizObject':
        if data:
            self.merge(data)

        prepared_record = self.internal.state.copy()
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
        self.internal.state.update(created_record)

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
        self.internal.state.update(updated_record)
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
            record = bizobj.internal.state.copy()
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
            bizobj.internal.state.update(record)
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
                bizobj.internal.state.update(record)
                bizobj.clean()

        return cls.BizList(bizobjs)

    def save(self, depth=1):
        if not depth:
            return self

        if self._id is None or '_id' in self.dirty:
            self.create()
        else:
            self.update()

        for rel in self.relationships.values():
            biz_thing = self.internal.attributes.get(rel.name)
            if biz_thing:
                biz_thing.save(depth=depth-1)

        return self

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
    def dirty_data(self) -> Dict:
        dirty_keys = self.dirty
        return {k: self.internal.state[k] for k in dirty_keys}

    @property
    def dirty(self) -> Set[Text]:
        return self.internal.state.dirty

    def clean(self, keys=None) -> 'BizObject':
        self.internal.state.clear_dirty(keys)
        return self

    def mark(self, keys) -> 'BizObject':
        if not is_sequence(keys):
            keys = {keys}
        self.internal.state.mark_dirty({k for k in keys if k in self.schema.fields})
        return self

    def copy(self, deep=False) -> 'BizObject':
        """
        Create a clone of this BizObject. Deep copy its fields but, by default.

        Args:
        - `deep`: If set, deep copy related BizObjects.
        """
        clone = self.__class__(deepcopy(self.internal.state))

        # select the copy method to use for relationship-loaded data
        copy_related_value = deepcopy if deep else copy

        # copy related BizObjects
        for k, v in self.related.items():
            if not self.relationships[k].many:
                clone.related[k] = copy_related_value(v)
            else:
                clone.related[k] = [copy_related_value(i) for i in v]

        return clone.clean()

    def merge(self, source=None, **source_kwargs) -> 'BizObject':
        """
        Merge another dict or BizObject's data dict into the data dict of this
        BizObject. Not called "update" because that would be confused as the
        name of the CRUD method.
        """
        if not (source or source_kwargs):
            return self

        if is_bizobj(source):
            assert isinstance(source, self.__class__)
            for k, v in source.internal.state.items():
                setattr(self, k, v)
            for k, v in source.internal.attributes.items():
                setattr(self, k, v)
        elif isinstance(source, dict):
            original_source = source
            source = self.schema.translate_source(source)
            for k, v in source.items():
                if k in self.schema.fields or k in self.attributes:
                    setattr(self, k, v)
            for k in original_source.keys() - source.keys():
                setattr(self, k, original_source[k])

        if source_kwargs:
            self.merge(source=source_kwargs)

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
            self.clean(fresh.internal.state.keys())

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
        for k in keys:
            if k in self.internal.state:
                self.internal.state.pop(k, None)
            elif k in self.internal.attributes:
                self.internal.attributes.pop(k, None)

    def is_loaded(self, keys: Set[Text]) -> bool:
        """
        Are all given field and/or relationship values loaded?
        """
        keys = {keys} if isinstance(keys, str) else keys
        for k in keys:
            if not (k in self.internal.state or k in self.internal.attributes):
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
