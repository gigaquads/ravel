import uuid

import venusian

from copy import deepcopy, copy
from typing import List, Dict, Text, Type, Tuple, Set
from collections import defaultdict

from pybiz.dao.dao_binder import DaoBinder
from pybiz.dao.python_dao import PythonDao
from pybiz.util import is_bizobj, is_sequence, repr_biz_id
from pybiz.util.loggers import console
from pybiz.dirty import DirtyDict
from pybiz.exc import ValidationError, BizObjectError

from .internal.biz_object_type_builder import BizObjectTypeBuilder
from .internal.save import SaveMethod, BreadthFirstSaver
from .internal.dump import NestingDumper, SideLoadingDumper
from .internal.query import Query, QueryUtils


class BizObjectMeta(type):

    builder = BizObjectTypeBuilder.get_instance()
    reserved_attrs = set()

    def __new__(cls, name, bases, ns):
        if name != 'BizObject':
            ns = BizObjectMeta.builder.prepare_class_attributes(name, bases, ns)
        else:
            BizObjectMeta.reserved_attrs = {
                k for k in ns if not k.startswith('_')
            } | {'_data', '_related', '_hash'}
        return type.__new__(cls, name, bases, ns)

    def __init__(biz_type, name, bases, ns):
        type.__init__(biz_type, name, bases, ns)
        if name != 'BizObject':
            BizObjectMeta.builder.initialize_class_attributes(name, biz_type)

            venusian.attach(
                biz_type, BizObjectMeta.venusian_callback, category='biz'
            )

            field_name_conflicts = (
                biz_type.schema.fields.keys() & BizObjectMeta.reserved_attrs
            )
            if field_name_conflicts:
                raise BizObjectError(
                    message=(
                        'tried to define field(s) with '
                        'reserved name: {}'.format(
                            ', '.join(field_name_conflicts)
                        )
                    )
                )
            rel_name_conflicts = (
                biz_type.relationships.keys() & BizObjectMeta.reserved_attrs
            )
            if rel_name_conflicts:
                raise BizObjectError(
                    message=(
                        'tried to define relationship(s) with '
                        'reserved names: {}'.format(
                            ', '.join(rel_name_conflicts)
                        )
                    )
                )

    @staticmethod
    def venusian_callback(scanner, name, biz_type):
        console.info(f'venusian detected {biz_type.__name__}')
        scanner.biz_types.setdefault(name, biz_type)


class BizObject(metaclass=BizObjectMeta):

    Schema = None
    BizList = None

    schema = None
    relationships = {}

    is_bootstrapped = False
    is_abstract = False

    binder = DaoBinder.get_instance()  # TODO: Make into property
    registry = None

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
    def bootstrap(cls, registry: 'Registry', **kwargs):
        cls.registry = registry
        for rel in cls.relationships.values():
            rel.bootstrap(registry)
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
            if not isinstance(order_by, (tuple, list)):
                order_by = (order_by, )
            query.spec.order_by = order_by

        console.debug(
            message='executing query',
            data={
                'class': cls.__name__,
                'predicate': predicate,
                'fields': query.spec.fields,
                'order_by': query.spec.order_by,
                'limit': query.spec.limit,
                'offset': query.spec.offset,
            }
        )

        results = query.execute()
        if first:
            return results[0] if results else None
        else:
            return cls.BizList(results)

    @classmethod
    def get(cls, _id, fields: Dict = None) -> 'BizObject':
        _id, err = cls.schema.fields['_id'].process(_id)
        if err:
            raise ValidationError(f'invalid _id {_id}: {err}')

        fields, children = QueryUtils.prepare_fields_argument(cls, fields)
        fields.update({'_id', '_rev'})

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
                raise ValidationError(f'invalid _id {_id}: {err}')

        # separate field names into those corresponding to this BizObjects
        # class and those of the related BizObject classes.
        fields, children = QueryUtils.prepare_fields_argument(cls, fields)
        fields.update({'_id', '_rev'})

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

    def delete(self) -> 'BizObject':
        """
        Call delete on this object's dao and therefore mark all fields as dirty
        and delete its _id so that save now triggers Dao.create.
        """
        self.dao.delete(_id=self._id)
        self.mark(self._data.keys())
        self._id = None
        return self

    @classmethod
    def delete_many(cls, bizobjs) -> None:
        bizobj_ids = []
        for obj in bizobjs:
            obj.mark(obj._data.keys())
            bizobj_ids.append(obj._id)
            obj._id = None
        cls.get_dao().delete_many(bizobj_ids)

    @classmethod
    def delete_all(cls) -> None:
        cls.get_dao().delete_all()

    def save(self, method: SaveMethod = None) -> 'BizObject':
        method = SaveMethod(method or SaveMethod.breadth_first)
        if method == SaveMethod.breadth_first:
            saver = BreadthFirstSaver(self.__class__)
        return saver.save_one(self)

    @classmethod
    def save_many(
        cls,
        bizobjs: List['BizObject'],
        method: SaveMethod = None,
    ) -> 'BizList':
        method = SaveMethod(method or SaveMethod.breadth_first)
        if method == SaveMethod.breadth_first:
            saver = BreadthFirstSaver(cls)
        return saver.save_many(bizobjs)

    def create(self) -> 'BizObject':
        prepared_record = self._data.copy()
        self.insert_defaults(prepared_record)
        prepared_record.pop('_rev', None)
        prepared_record, errors = self.schema.process(prepared_record)
        if errors:
            raise ValidationError(
                message=f'could not create {self.__class__.__name__} object',
                data=errors
            )
        created_record = self.get_dao().create(prepared_record)
        self._data.update(created_record)
        return self.clean()

    def update(self, data: Dict = None, **more_data) -> 'BizObject':
        data = dict(data or {}, **more_data)
        if data:
            self.merge(data)
        raw_record = self.dirty_data
        raw_record.pop('_rev', None)
        raw_record.pop('_id', None)
        prepared_record, errors = self.schema.process(raw_record)
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
        self._data.update(updated_record)
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
            record = bizobj._data.copy()
            cls.insert_defaults(record)
            record, errors = cls.schema.process(record)
            record.pop('_rev', None)
            if errors:
                raise ValidationError(
                    message=(
                        f'could not create {self.__class__.__name__} object'
                    ),
                    data=errors
                )
            records.append(record)

        created_records = cls.get_dao().create_many(records)

        for bizobj, record in zip(bizobjs, created_records):
            bizobj._data.update(record)
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
                bizobj._data.update(record)
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

        if generated_defaults:
            console.debug(
                message='generated default values',
                data={
                    'type': cls.__name__,
                    'defaults': generated_defaults,
                }
            )

    @property
    def dao(self) -> 'Dao':
        return self.get_dao()

    @property
    def raw(self) -> 'DirtyDict':
        return self._data

    @property
    def dirty_data(self) -> Dict:
        dirty_keys = self.dirty
        return {k: self._data[k] for k in dirty_keys}

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
        return self

    def copy(self, deep=False) -> 'BizObject':
        """
        Create a clone of this BizObject. Deep copy its fields but, by default.

        Args:
        - `deep`: If set, deep copy related BizObjects.
        """
        clone = self.__class__(deepcopy(self._data))

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
            dirty_keys = obj._data.keys()
            for k, v in obj._data.items():
                setattr(self, k, v)
            for k, v in obj._related.items():
                rel = self.relationships[k]
                rel.set_internally(self, v)
        elif isinstance(obj, dict):
            dirty_keys = obj.keys()
            for k, v in obj.items():
                rel = self.relationships.get(k)
                if rel:
                    rel.set_internally(self, v)
                else:
                    setattr(self, k, v)

        if more_data:
            for k, v in more_data.items():
                rel = self.relationships.get(k)
                if rel:
                    rel.set_internally(self, v)
                else:
                    setattr(self, k, v)

        return self

    def load(self, keys=None) -> 'BizObject':
        """
        Assuming _id is not None, this will load the rest of the BizObject's
        data. By default, relationship data is not loaded unless explicitly
        requested.
        """
        if isinstance(keys, str):
            keys = {keys}
        console.debug(message='loading', data={
            'class': self.__class__.__name__,
            'instance': self._id,
            'keys': keys
        })
        fresh = self.get(_id=self._id, fields=keys)
        self.merge(fresh)
        self.clean(keys)
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
            if k in self._data:
                self._data.pop(k, None)
            elif k in self._related:
                self._related.pop(k, None)

    def is_loaded(self, keys: Set[Text]) -> bool:
        """
        Are all given field and/or relationship values loaded?
        """
        keys = {keys} if isinstance(keys, str) else keys
        for k in keys:
            if not (k in self._data or k in self._related):
                return False
        return True

    def dump(self, fields=None, raw=False, style='nested') -> Dict:
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

        result = dump(target=self, fields=fields, raw=raw)
        return result
