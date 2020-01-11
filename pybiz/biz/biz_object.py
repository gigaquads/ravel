import inspect
import uuid

from typing import Text, Tuple, List, Set, Dict, Type
from pprint import pprint
from collections import defaultdict
from copy import deepcopy

import venusian

from appyratus.utils import DictObject
from appyratus.enum import EnumValueStr

from pybiz.util.misc_functions import (
    is_sequence,
    get_class_name,
    repr_biz_id,
    flatten_sequence,
)
from pybiz.schema import (
    Field, Schema, fields, String, Int, Id, UuidString,
)
from pybiz.dao import Dao, PythonDao
from pybiz.util.loggers import console
from pybiz.exceptions import ValidationError
from pybiz.constants import (
    ID_FIELD_NAME,
    REV_FIELD_NAME,
)

from .util import is_biz_list, is_biz_object
from .biz_thing import BizThing
from .biz_list import BizList
from .dirty import DirtyDict
from .dumper import Dumper, NestedDumper, SideLoadedDumper, DumpStyle
from .query.query import Query
from .query.request import QueryRequest
from .field_resolver import FieldResolver, FieldResolverProperty
from .resolver.resolver import Resolver
from .resolver.resolver_property import ResolverProperty
from .resolver.resolver_decorator import ResolverDecorator
from .resolver.resolver_manager import ResolverManager


class BizObjectMeta(type):
    def __init__(biz_class, name, bases, attr_dict):
        super().__init__(name, bases, attr_dict)
        info = biz_class._analyze()
        fields = info['fields']
        resolvers = info['resolvers']
        resolver_decorators = info['resolver_decorators']

        biz_class._pybiz_is_biz_object = True
        biz_class._init_pybiz_dict_object(info)
        biz_class._compute_is_abstract()
        biz_class._build_schema_class(fields, bases)
        biz_class._build_field_resolvers(resolvers)
        biz_class._build_resolvers(bases, resolvers, resolver_decorators)
        biz_class._build_resolver_properties()
        biz_class._build_biz_list()
        biz_class._extract_field_defaults()

        def callback(scanner, name, biz_class):
            """
            Callback used by Venusian for BizObject class auto-discovery.
            """
            console.info(f'venusian scan found "{biz_class.__name__}" BizObject')
            scanner.biz_classes.setdefault(name, biz_class)

        venusian.attach(biz_class, callback, category='biz')

    def _analyze(biz_class):
        # TODO: structure this up
        info = {
            'fields': {},
            'resolvers': {},
            'resolver_decorators': {},
        }
        for k, v in inspect.getmembers(biz_class):
            if isinstance(v, Field):
                info['fields'][k] = v
                v.name = k
            elif isinstance(v, Resolver):
                info['resolvers'][k] = v
                v.name = k
            elif isinstance(v, ResolverDecorator):
                info['resolver_decorators'][k] = v

        return info

    @staticmethod
    def _is_biz_class(class_obj):
        class_data = getattr(class_obj, 'pybiz', None)
        return class_data and getattr(class_data, 'is_biz_object', False)

    def _build_resolvers(
        biz_class,
        base_classes,
        resolvers_dict,
        resolver_decorators
    ):
        resolvers = biz_class.pybiz.resolvers

        for resolver in resolvers_dict.values():
            resolver.biz_class = biz_class
            resolvers.register(resolver)

        # inherit Resolvers
        for base_class in base_classes:
            if biz_class._is_biz_class(base_class):
                for resolver in base_class.pybiz.resolvers.values():
                    resolver_copy = resolver.copy()
                    resolver_copy.biz_class = biz_class
                    resolvers.register(resolver_copy)

        # build Resolvers, assembled by ResolverDecorators
        for name, dec in resolver_decorators.items():
            resolver = dec.resolver_class(
                biz_class=biz_class,
                name=name,
                on_execute=dec.on_execute_func,
                on_get=dec.on_get_func,
                on_set=dec.on_set_func,
                on_del=dec.on_del_func,
                **dec.kwargs,
            )
            resolvers.register(resolver)

    def _init_pybiz_dict_object(biz_class, info):
        biz_class.pybiz = DictObject()
        biz_class.pybiz.app = None
        biz_class.pybiz.dao = None
        biz_class.pybiz.resolvers = ResolverManager()
        biz_class.pybiz.fk_id_fields = {}
        biz_class.pybiz.is_biz_object = True
        biz_class.pybiz.is_abstract = False
        biz_class.pybiz.is_bootstrapped = False
        biz_class.pybiz.is_bound = False
        biz_class.pybiz.schema = None
        biz_class.pybiz.defaults = {}

    def _compute_is_abstract(biz_class):
        if hasattr(biz_class, '__abstract__'):
            biz_class.pybiz.is_abstract = biz_class.__abstract__()
            delattr(biz_class, '__abstract__')
        else:
            biz_class.pybiz.is_abstract = False

    def _build_schema_class(biz_class, fields, base_classes):
        def extract_fields(class_obj):
            fields = {}
            is_field = lambda x: isinstance(x, Field)
            for k, field in inspect.getmembers(class_obj, predicate=is_field):
                if k.startswith('_pybiz'):
                    continue
                fields[k] = deepcopy(field)
            return fields

        fields = fields.copy()

        # inherit Fields from base BizObject classes
        for base_class in base_classes:
            if biz_class._is_biz_class(base_class):
                fields.update(deepcopy(base_class.Schema.fields))
                biz_class.pybiz.defaults.update(base_class.pybiz.defaults)

        fields.update(extract_fields(biz_class))

        for k, field in fields.items():
            if field.source is None:
                field.source = field.name
            if isinstance(field, Id) and field.name != ID_FIELD_NAME:
                    biz_class.pybiz.fk_id_fields[field.name] = field

        class_name = f'{biz_class.__name__}Schema'

        assert ID_FIELD_NAME in fields
        assert REV_FIELD_NAME in fields

        id_field = fields[ID_FIELD_NAME]

        if isinstance(id_field, Id):
            replacement_id_field = biz_class.id_field_factory()
            replacement_id_field.required = True
            replacement_id_field.name = ID_FIELD_NAME
            replacement_id_field.source = id_field.source or ID_FIELD_NAME
            replacement_id_field.meta.update(id_field.meta)
            fields[ID_FIELD_NAME] = replacement_id_field

        biz_class.Schema = type(class_name, (Schema, ), fields)
        biz_class.pybiz.schema = biz_class.Schema()

    def _build_field_resolvers(biz_class, resolvers: dict):
        for k, field in biz_class.Schema.fields.items():
            resolver = FieldResolver(field, name=k)
            resolvers[k] = resolver

    def _build_resolver_properties(biz_class):
        for resolver in biz_class.pybiz.resolvers.values():
            if resolver.name in biz_class.pybiz.resolvers.fields:
                resolver_prop = FieldResolverProperty(resolver)
            else:
                resolver_prop = ResolverProperty(resolver)
            setattr(biz_class, resolver.name, resolver_prop)

    def _build_biz_list(biz_class):
        class CustomBizList(BizList):
            pass

        CustomBizList.pybiz.biz_class = biz_class
        biz_class.BizList = CustomBizList

    def _extract_field_defaults(biz_class):
        def build_default_func(field):
            if callable(field.default):
                return field.default
            else:
                return lambda: deepcopy(field.default)

        defaults = biz_class.pybiz.defaults
        for field in biz_class.Schema.fields.values():
            if field.default:
                defaults[field.name] = build_default_func(field)
                field.default = None


class BizObject(BizThing, metaclass=BizObjectMeta):

    # internal pybiz class-level data goes in cls.pybiz and is built by the
    # BizObjectMeta metaclass.
    pybiz = None

    # these aliases are also build by the metaclass. Schema is the Schema
    # containing all fields defined on this class as well as inherited. List is
    # a BizList class dynamically build around this class.
    Schema = None
    BizList = None

    # built-in fields
    _id = Id()
    _rev = String()

    def __init__(self, data: Dict = None, **more_data):
        self.internal = DictObject()
        self.internal.state = DirtyDict()
        self.internal.resolvers = ResolverManager()

        # merge more_data into data
        data = data or {}
        if more_data:
            data.update(more_data)

        # unlike other fields, whose defaults are generated upon calling
        # self.create or cls.create_many, the _id field default is generated up
        # front so that, as much as possible, other BizObjects can access this
        # object by _id when defining relationships and such.
        if ID_FIELD_NAME not in data:
            if ID_FIELD_NAME in self.pybiz.defaults:
                # TODO: why doesnt this raise if _id not in defaults
                data[ID_FIELD_NAME] = self.pybiz.defaults['_id']()

        self.merge(data)

    def __getitem__(self, key):
        if key in self.Schema.fields:
            return getattr(self, key)
        raise KeyError(key)

    def __setitem__(self, key, value):
        if key in self.pybiz.all_selectors:
            return setattr(self, key, value)
        raise KeyError(key)

    def __delitem__(self, key):
        if key in self.pybiz.all_selectors:
            delattr(self, key)
        else:
            raise KeyError(key)

    def __getattribute__(self, key):
        try:
            return super().__getattribute__(key)
        except AttributeError as exc:
            # chance to handle the attribute differently
            # if not, re-raise the exception
            if key in self.internal.state:
                return self.internal.state[key]
            raise exc

    def __iter__(self):
        return iter(self.internal.state)

    def __contains__(self, key):
        return key in self.internal.state

    def __repr__(self):
        id_str = repr_biz_id(self)
        name = get_class_name(self)
        dirty = '*' if self.internal.state.dirty else ''
        return f'<{name}({id_str}){dirty}>'

    @classmethod
    def __abstract__(cls) -> bool:
        return True

    @classmethod
    def __dao__(cls) -> Type[Dao]:
        return PythonDao

    @classmethod
    def on_bootstrap(cls, app, *args, **kwargs):
        pass

    @classmethod
    def on_bind(cls):
        pass

    @classmethod
    def bootstrap(cls, app, *args, **kwargs):
        cls.pybiz.app = app

        # resolve the concrete Field class to use for each "foreign key"
        # ID field referenced by this class.
        for id_field in cls.pybiz.fk_id_fields.values():
            id_field.replace_self_in_biz_class(app, cls)

        # bootstrap all resolvers owned by this class
        for resolver in cls.pybiz.resolvers.values():
            resolver.bootstrap(cls)

        # lastly perform custom developer logic
        cls.on_bootstrap(app, *args, **kwargs)
        cls.pybiz.is_bootstrapped = True

    @classmethod
    def bind(cls, binder: 'ApplicationDaoBinder', **kwargs):
        cls.binder = binder
        cls.pybiz.dao = cls.pybiz.app.binder.get_binding(cls).dao_instance
        for resolver in cls.pybiz.resolvers.values():
            resolver.bind(cls)
        cls.pybiz.is_bound = True
        cls.on_bind()

    @classmethod
    def is_bootstrapped(cls) -> bool:
        return cls.pybiz.is_bootstrapped

    @classmethod
    def is_bound(cls) -> bool:
        return cls.pybiz.is_bound

    @classmethod
    def id_field_factory(cls) -> 'Field':
        return UuidString(default=lambda: uuid.uuid4().hex)

    @classmethod
    def get_dao(cls, bind=True) -> 'Dao':
        """
        Get the global Dao reference associated with this class.
        """
        return cls.pybiz.dao

    @classmethod
    def select(
        cls,
        *targets: Tuple['ResolverProperty'],
        **subqueries: Dict[Text, 'Query']
    ) -> 'Query':
        flattened_targets = flatten_sequence(targets)
        return Query(cls).select(flattened_targets, **subqueries)

    @property
    def dao(self) -> 'Dao':
        return self.get_dao()

    @property
    def dirty(self) -> Set[Text]:
        return {
            k: self.internal.state[k]
            for k in self.internal.state.dirty
            if k in self.Schema.fields
        }

    def pprint(self):
        pprint(self.internal.state)

    def clean(self, fields=None) -> 'BizObject':
        if fields is not None:
            if not fields:
                return self
            fields = fields if is_sequence(fields) else {fields}
            keys = self._normalize_selectors(fields)
        else:
            keys = set(self.pybiz.resolvers.keys())

        self.internal.state.clean(keys=keys)
        return self

    def mark(self, fields=None) -> 'BizObject':
        # TODO: rename to "touch"
        if fields is not None:
            if not fields:
                return self
            fields = fields if is_sequence(fields) else {fields}
            keys = self._normalize_selectors(fields)
        else:
            keys = set(self.Schema.fields.keys())

        self.internal.state.mark(keys)
        return self

    def copy(self) -> 'BizObject':
        """
        Create a clone of this BizObject
        """
        clone = type(self)(data=deepcopy(self.internal.state))
        clone.internal.resolvers = self.internal.resolvers.copy()
        return clone.clean()

    def merge(self, other=None, **values) -> 'BizObject':
        if isinstance(other, dict):
            for k, v in other.items():
                setattr(self, k, v)
        elif isinstance(other, BizObject):
            for k, v in other.internal.state.items():
                setattr(self, k, v)

        if values:
            self.merge(values)

        return self

    def load(self, selectors=None) -> 'BizObject':
        if self._id is None:
            return self

        if isinstance(selectors, str):
            selectors = {selectors}

        # TODO: fix up Query so that even if the fresh object does exist in the
        # DAL, it will still try to execute the resolvers on the uncreated
        # object.

        # resolve a fresh copy throught the DAL and merge state
        # into this BizObject.
        query = self.select(selectors).where(_id=self._id)
        fresh = query.execute(first=True)
        if fresh:
            self.merge(fresh)
            self.clean(fresh.internal.state.keys())

        return self

    def reload(self, selectors=None) -> 'BizObject':
        if isinstance(keys, str):
            keys = {keys}
        keys = {k for k in keys if self.is_loaded(k)}
        return self.load(keys)

    def unload(self, selectors: Set) -> 'BizObject':
        """
        Remove the given keys from field data and/or relationship data.
        """
        if selectors:
            if isinstance(selectors, str):
                selectors = {selectors}
                keys = self._normalize_selectors(selectors)
        else:
            keys = set(
                self.internal.state.keys() |
                self.pybiz.resolvers.keys()
            )
        for k in keys:
            if k in self.internal.state:
                del self.internal.state[k]
            elif k in self.pybiz.resolvers:
                del self.pybiz.resolvers[k]

    def is_loaded(self, selectors: Set) -> bool:
        """
        Are all given field and/or relationship values loaded?
        """
        if selectors:
            if isinstance(selectors, str):
                selectors = {selectors}
                keys = self._normalize_selectors(selectors)
        else:
            keys = set(
                self.internal.state.keys() |
                self.pybiz.resolvers.keys()
            )

        for k in keys:
            is_key_in_data = k in self.internal.state
            is_key_in_resolvers = k in self.pybiz.resolvers
            if not (is_key_in_data or is_key_in_resolvers):
                return False

        return True

    def resolve(self, *selectors):
        keys = self._normalize_selectors(selectors)
        if not keys:
            keys = self.pybiz.resolvers.keys()

        data = {}
        for key in keys:
            resolver = self.pybiz.resolvers[key]
            resolver_query = resolver.select()
            request = QueryRequest(resolver_query, self, resolver=resolver)
            data[key] = resolver_query.execute(request)

        self.merge(data)
        return self

    def dump(self, resolvers: Set[Text] = None, style: DumpStyle = None) -> Dict:
        """
        Dump the fields of this business object along with its related objects
        (declared as relationships) to a plain ol' dict.
        """
        # get Dumper instance based on DumpStyle (nested, side-loaded, etc)
        dumper = Dumper.for_style(style or DumpStyle.nested)

        if resolvers is not None:
            # only dump resolver state specifically requested
            keys_to_dump = self._normalize_selectors(resolvers)
        else:
            # or else dump all instance state
            keys_to_dump = list(self.internal.state.keys())

        dumped_instance_state = dumper.dump(self, keys=keys_to_dump)
        return dumped_instance_state

    @classmethod
    def query(
        cls,
        select: Set[Text] = None,
        where: 'Predicate' = None,
        order_by: Tuple[Text] = None,
        offset: int = None,
        limit: int = None,
        custom: Dict = None,
        execute=True,
        first=False,
    ):
        """
        Alternate syntax for building Query objects manually.
        """
        # select all BizObject fields by default
        query = Query(cls)

        if select:
            query.select(select)
        if where:
            query.where(where)
        if order_by:
            query.order_by(order_by)
        if limit is not None:
            query.limit(limit)
        if offset is not None:
            query.offset(offset)
        if custom:
            query.params.custom = custom

        if not execute:
            return query
        else:
            return query.execute(first=first)

    @classmethod
    def get(cls, _id, select=None) -> 'BizObject':
        if _id is None:
            return None
        if not select:
            data = cls.get_dao().fetch(_id)
            return cls(data=data).clean() if data else None
        else:
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
        if not _ids:
            return cls.BizList()
        if not (select or offset or limit or order_by):
            dao = cls.get_dao()
            id_2_data = dao.dispatch('fetch_many', (_ids, ))
            return cls.BizList(cls(data=data) for data in id_2_data.values())
        else:
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
        self.dao.dispatch('delete', (self._id, ))
        self.mark(self.internal.state.keys())
        self._id = None
        return self

    @classmethod
    def delete_many(cls, biz_objs) -> None:
        # extract ID's of all objects to delete and clear
        # them from the instance objects' state dicts
        biz_obj_ids = []
        for obj in biz_objs:
            obj.mark(obj.internal.state.keys())
            biz_obj_ids.append(obj._id)
            obj._id = None

        # delete the records in the DAL
        dao = cls.get_dao()
        dao.dispatch('delete_many', args=(biz_obj_ids, ))

    @classmethod
    def delete_all(cls) -> None:
        dao = cls.get_dao()
        dao.dispatch('delete_all')

    def exists(self) -> bool:
        """
        Does a simple check if a BizObject exists by id.
        """
        if self._id is not None:
            return self.dao.dispatch('exists', args=(self._id, ))
        return False

    def save(self, depth=0):
        return self.save_many([self], depth=depth)[0]

    def create(self, data: Dict = None) -> 'BizObject':
        if data:
            self.merge(data)

        prepared_record = self._prepare_record_for_create()
        prepared_record.pop(REV_FIELD_NAME, None)

        created_record = self.dao.dispatch('create', (prepared_record, ))

        self.internal.state.update(created_record)
        return self.clean()

    def update(self, data: Dict = None, **more_data) -> 'BizObject':
        data = dict(data or {}, **more_data)
        if data:
            self.merge(data)

        raw_record = self.dirty.copy()
        raw_record.pop(REV_FIELD_NAME, None)
        raw_record.pop(ID_FIELD_NAME, None)

        errors = {}
        prepared_record = {}
        for k, v in raw_record.items():
            field = self.Schema.fields.get(k)
            if field is not None:
                prepared_record[k], error = field.process(v)
                if error:
                    errors[k] = error

        if errors:
            raise ValidationError(
                message=f'could not update {get_class_name(self)} object',
                data={
                    ID_FIELD_NAME: self._id,
                    'errors': errors,
                }
            )

        updated_record = self.dao.dispatch(
            'update', (self._id, prepared_record)
        )

        self.internal.state.update(updated_record)
        return self.clean()

    @classmethod
    def create_many(cls, biz_objs: List['BizObject']) -> 'BizList':
        """
        Call `dao.create_method` on input `BizObject` list and return them in
        the form of a BizList.
        """
        records = []

        for biz_obj in biz_objs:
            if biz_obj is None:
                continue
            if isinstance(biz_obj, dict):
                biz_obj = cls(data=biz_obj)

            record = biz_obj._prepare_record_for_create()
            records.append(record)

        dao = cls.get_dao()
        created_records = dao.dispatch('create_many', (records, ))

        for biz_obj, record in zip(biz_objs, created_records):
            biz_obj.internal.state.update(record)
            biz_obj.clean()

        return cls.BizList(biz_objs)

    @classmethod
    def update_many(
        cls, biz_objs: List['BizObject'], data: Dict = None, **more_data
    ) -> 'BizList':
        """
        Call the Dao's update_many method on the list of BizObjects. Multiple
        Dao calls may be made. As a preprocessing step, the input biz_obj list
        is partitioned into groups, according to which subset of fields are
        dirty.

        For example, consider this list of biz_objs,

        ```python
        biz_objs = [
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

        A spearate DAO call to `update_many` will be made for each partition.
        """
        # common_values are values that should be updated
        # across all objects.
        common_values = dict(data or {}, **more_data)

        # in the procedure below, we partition all incoming BizObjects
        # into groups, grouped by the set of fields being updated. In this way,
        # we issue an update_many datament for each partition in the DAL.
        partitions = defaultdict(list)

        for biz_obj in biz_objs:
            if biz_obj is None:
                continue
            if common_values:
                biz_obj.merge(common_values)
            partitions[tuple(biz_obj.dirty)].append(biz_obj)

        for biz_obj_partition in partitions.values():
            records, _ids = [], []

            for biz_obj in biz_obj_partition:
                record = biz_obj.dirty.copy()
                record.pop(REV_FIELD_NAME, None)
                record.pop(ID_FIELD_NAME, None)
                records.append(record)
                _ids.append(biz_obj._id)

            dao = cls.get_dao()
            updated_records = dao.dispatch('update_many', (_ids, records))

            for biz_obj, record in zip(biz_obj_partition, updated_records):
                biz_obj.internal.state.update(record)
                biz_obj.clean()

        return cls.BizList(biz_objs)

    @classmethod
    def save_many(
        cls,
        biz_objects: List['BizObject'],
        depth: int = 0
    ) -> 'BizList':
        """
        Essentially a bulk upsert.
        """
        def seems_created(biz_obj):
            return (
                (ID_FIELD_NAME in biz_obj.internal.state) and
                (ID_FIELD_NAME not in biz_obj.internal.state.dirty)
            )

        # partition biz_objects into those that are "uncreated" and those which
        # simply need to be updated.
        to_update = []
        to_create = []
        for biz_obj in biz_objects:
            # TODO: merge duplicates
            if not seems_created(biz_obj):
                to_create.append(biz_obj)
            else:
                to_update.append(biz_obj)

        # perform bulk create and update
        if to_create:
            created = cls.create_many(to_create)
        if to_update:
            updated = cls.update_many(to_update)

        retval = cls.BizList(to_update + to_create)

        if depth < 1:
            # base case. do not recurse on Resolvers
            return retval

        # aggregate and save all BizObjects referenced by all objects in
        # `biz_object` via their resolvers.
        class_2_objects = defaultdict(set)
        resolvers = cls.pybiz.resolvers.by_tag('fields', invert=True)
        for resolver in resolvers.values():
            for biz_obj in biz_objects:
                if resolver.name in biz_obj.internal.state:
                    value = biz_obj.internal.state[resolver.name]
                    biz_thing_to_save = resolver.on_save(resolver, biz_obj, value)
                    if biz_thing_to_save:
                        if is_biz_object(biz_thing_to_save):
                            class_2_objects[resolver.biz_class].add(
                                biz_thing_to_save
                            )
                        else:
                            assert is_sequence(biz_thing_to_save)
                            class_2_objects[resolver.biz_class].update(
                                biz_thing_to_save
                            )

        # recursively call save_many for each type of BizObject
        for biz_class, biz_objects in class_2_objects.items():
            biz_class.save_many(biz_objects, depth=depth-1)

        return retval

    @classmethod
    def generate(cls, query: Query = None) -> 'BizObject':
        instance = cls()
        query = query or cls.select(cls.pybiz.resolvers.fields)
        resolvers = Resolver.sort(
            cls.pybiz.resolvers[k] for k in query.params.select
        )
        for resolver in resolvers:
            if resolver.name == REV_FIELD_NAME:
                setattr(instance, resolver.name, '0')
            else:
                resolver_query = query.params.select[resolver.name]
                generated_value = resolver.generate(instance, resolver_query)
                setattr(instance, resolver.name, generated_value)
        return instance

    def _prepare_record_for_create(self):
        """
        Prepares a a BizObject state dict for insertion via DAL.
        """
        # extract only those elements of state data that correspond to
        # Fields declared on this BizObject class.
        record = {
            k: v for k, v in self.internal.state.items()
            if k in self.pybiz.resolvers.fields
        }
        # when inserting or updating, we don't want to write the _rev value on
        # accident. The DAL is solely responsible for modifying this value.
        if REV_FIELD_NAME in record:
            del record[REV_FIELD_NAME]

        # generate default values for any missing fields
        # that specifify a default
        for k, default in self.pybiz.defaults.items():
            if k not in record:
                def_val = default()
                record[k] = def_val

        return record

    @staticmethod
    def _normalize_selectors(selectors: Set):
        keys = set()
        for k in selectors:
            if isinstance(k, str):
                keys.add(k)
            elif isinstance(k, ResolverProperty):
                keys.add(k.name)
        return keys
