import inspect
import uuid

from typing import Text, Tuple, List, Set, Dict, Type, Union
from collections import defaultdict
from copy import deepcopy

from appyratus.utils import DictObject

from ravel.exceptions import ValidationError
from ravel.store import Store, SimulationStore
from ravel.util.dirty import DirtyDict
from ravel.util.loggers import console
from ravel.util import (
    is_batch,
    is_resource,
    is_resource_type,
    is_sequence,
    get_class_name,
    flatten_sequence,
    union,
)
from ravel.schema import (
    Field, Schema, String, Int, Id,
    UuidString, Bool, Float
)
from ravel.dumper import Dumper, NestedDumper, SideLoadedDumper, DumpStyle
from ravel.batch import Batch
from ravel.constants import (
    IS_RESOURCE,
    ID,
    REV,
)

from ravel.query.query import Query
from ravel.query.order_by import OrderBy
from ravel.resolver.resolver import Resolver
from ravel.resolver.resolver_decorator import ResolverDecorator
from ravel.resolver.resolver_property import ResolverProperty
from ravel.resolver.resolver_manager import ResolverManager
from ravel.resolver.resolvers.loader import Loader, LoaderProperty
from ravel.entity import Entity


class ResourceMeta(type):
    def __init__(cls, name, bases, dct):
        cls._initialize_class_state()

        fields = cls._process_fields()

        cls._build_schema_type(fields, bases)
        cls.Batch = Batch.factory(cls)

    def _initialize_class_state(resource_type):
        setattr(resource_type, IS_RESOURCE, True)

        resource_type.ravel = DictObject()
        resource_type.ravel.app = None
        resource_type.ravel.store = None
        resource_type.ravel.resolvers = ResolverManager()
        resource_type.ravel.foreign_keys = {}
        resource_type.ravel.is_abstract = resource_type._compute_is_abstract()
        resource_type.ravel.is_bootstrapped = False
        resource_type.ravel.is_bound = False
        resource_type.ravel.schema = None
        resource_type.ravel.defaults = {}

    def _process_fields(cls):
        fields = {}
        for k, v in inspect.getmembers(cls):
            if isinstance(v, ResolverDecorator):
                resolver_property = v.build_property(owner=cls, name=k)
                resolver = resolver_property.resolver
                cls.ravel.resolvers.register(resolver_property.resolver)
                setattr(cls, k, resolver_property)
                if isinstance(resolver, Loader):
                    fields[k] = resolver.field
            elif isinstance(v, Field):
                field = v
                field.name = k
                fields[k] = field
                resolver_property = Loader.build_property(
                    kwargs=dict(owner=cls, field=field, name=k, target=cls),
                )
                cls.ravel.resolvers.register(resolver_property.resolver)
                setattr(cls, k, resolver_property)
        return fields

    def _compute_is_abstract(resource_type):
        is_abstract = False
        if hasattr(resource_type, '__abstract__'):
            is_abstract = bool(resource_type.__abstract__())
            delattr(resource_type, '__abstract__')
        return is_abstract

    def _build_schema_type(resource_type, fields, base_types):
        fields = fields.copy()
        inherited_fields = {}

        # inherit fields and defaults from base Resource classes
        for base_type in base_types:
            if is_resource_type(base_type):
                inherited_fields.update(base_type.Schema.fields)
                resource_type.ravel.defaults.update(base_type.ravel.defaults)
            else:
                base_fields = resource_type._copy_fields_from_mixin(base_type)
                inherited_fields.update(base_fields)

        fields.update(inherited_fields)

        # perform final processing now that we have all direct and
        # inherited fields in one dict.
        for k, field in fields.items():
            if k in inherited_fields:
                resolver_property = Loader.build_property(kwargs=dict(
                    owner=resource_type, field=field, name=k, target=resource_type,
                ))
                resource_type.ravel.resolvers.register(resolver_property.resolver)
                setattr(resource_type, k, resolver_property)
            if field.source is None:
                field.source = field.name
            if isinstance(field, Id) and field.name != ID:
                    resource_type.ravel.foreign_keys[field.name] = field

        # these are universally required
        assert ID in fields
        assert REV in fields

        fields[ID].nullable = False

        # build new Schema subclass with aggregated fields
        class_name = f'{resource_type.__name__}Schema'
        resource_type.Schema = type(class_name, (Schema, ), fields)

        resource_type.ravel.schema = schema = resource_type.Schema()
        resource_type.ravel.defaults = resource_type._extract_field_defaults(schema)

    def _copy_fields_from_mixin(resource_type, class_obj):
        fields = {}
        is_field = lambda x: isinstance(x, Field)
        for k, field in inspect.getmembers(class_obj, predicate=is_field):
            if k == 'Schema':
                continue
            fields[k] = deepcopy(field)
        return fields

    def _extract_field_defaults(resource_type, schema):
        defaults = resource_type.ravel.defaults
        for field in schema.fields.values():
            if field.default:
                # move field default into "defaults" dict
                if callable(field.default):
                    defaults[field.name] = field.default
                else:
                    defaults[field.name] = lambda: deepcopy(field.default)
                # clear it from the schema object once "defaults" dict
                field.default = None
        return defaults


class Resource(Entity, metaclass=ResourceMeta):

    _id = UuidString(default=lambda: uuid.uuid4().hex, nullable=False)
    _rev = String()

    def __init__(self, state=None, **more_state):
        # initialize internal state data dict
        self.internal = DictObject()
        self.internal.state = DirtyDict()
        self.merge(state, **more_state)

        # eagerly generate default ID if none provided
        if ID not in self.internal.state:
            id_func = self.ravel.defaults.get(ID)
            self.internal.state[ID] = id_func() if id_func else None

    def __getitem__(self, key):
        if key in self.ravel.resolvers:
            return getattr(self, key)
        raise KeyError(key)

    def __setitem__(self, key, value):
        if key in self.ravel.resolvers:
            return setattr(self, key, value)
        raise KeyError(key)

    def __delitem__(self, key):
        if key in self.ravel.resolvers:
            delattr(self, key)
        else:
            raise KeyError(key)

    def __iter__(self):
        return iter(self.internal.state)

    def __contains__(self, key):
        return key in self.internal.state

    def __repr__(self):
        name = get_class_name(self)
        dirty = '*' if self.is_dirty else ''
        id_value = self.internal.state.get(ID)
        if id_value is None:
            id_str = '?'
        elif isinstance(id_value, str):
            id_str = id_value[:7]
        elif isinstance(id_value, UUID):
            id_str = id_value.hex[:7]
        else:
            id_str = repr(id_value)

        return f'{name}({id_str}){dirty}'

    @classmethod
    def __abstract__(cls) -> bool:
        return True

    @classmethod
    def __store__(cls) -> Type[Store]:
        return SimulationStore

    @classmethod
    def on_bootstrap(cls, app, *args, **kwargs):
        pass

    @classmethod
    def on_bind(cls):
        pass

    @classmethod
    def bootstrap(cls, app, *args, **kwargs):
        cls.ravel.app = app

        # resolve the concrete Field class to use for each "foreign key"
        # ID field referenced by this class.
        for id_field in cls.ravel.foreign_keys.values():
            id_field.replace_self_in_resource_type(app, cls)

        # bootstrap all resolvers owned by this class
        for resolver in cls.ravel.resolvers.values():
            resolver.bootstrap(app)

        # lastly perform custom developer logic
        cls.on_bootstrap(app, *args, **kwargs)
        cls.ravel.is_bootstrapped = True

    @classmethod
    def bind(cls, binder: 'ResourceBinder', **kwargs):
        cls.ravel.store = cls.ravel.app.binder.get_binding(cls).store_instance
        for resolver in cls.ravel.resolvers.values():
            resolver.bind()
        cls.on_bind()
        cls.ravel.is_bound = True

    @classmethod
    def is_bootstrapped(cls) -> bool:
        return cls.ravel.is_bootstrapped

    @classmethod
    def is_bound(cls) -> bool:
        return cls.ravel.is_bound

    @property
    def store(self) -> 'Store':
        return self.ravel.store

    @property
    def is_dirty(self) -> bool:
        return bool(
            self.internal.state.dirty &
            self.ravel.schema.fields.keys()
        )

    @property
    def dirty(self) -> Set[Text]:
        return {
            k: self.internal.state[k]
            for k in self.internal.state.dirty
            if k in self.Schema.fields
        }

    @classmethod
    def generate(cls, resolvers: Set[Text] = None, values: Dict = None) -> 'Resource':
        values = values or {}
        keys = resolvers or set(cls.ravel.resolvers.fields.keys())
        resolver_objs = Resolver.sort(
            cls.ravel.resolvers[k] for k in keys
            if k not in {REV}
        )

        instance = cls()

        for resolver in resolver_objs:
            if resolver.name in values:
                value = values[resolver.name]
            else:
                request = getattr(cls, resolver.name).select()
                value = resolver.simulate(instance, request)
            instance.internal.state[resolver.name] = value

        return instance

    def merge(self, other=None, **values) -> 'Resource':
        if isinstance(other, dict):
            for k, v in other.items():
                setattr(self, k, v)
        elif isinstance(other, Resource):
            for k, v in other.internal.state.items():
                setattr(self, k, v)

        if values:
            self.merge(values)

        return self

    def clean(self, fields=None) -> 'Resource':
        if fields:
            fields = fields if is_sequence(fields) else {fields}
            keys = self._normalize_selectors(fields)
        else:
            keys = set(self.ravel.resolvers.keys())

        if keys:
            self.internal.state.clean(keys=keys)

        return self

    def mark(self, fields=None) -> 'Resource':
        # TODO: rename "mark" method to "touch"
        if fields is not None:
            if not fields:
                return self
            fields = fields if is_sequence(fields) else {fields}
            keys = self._normalize_selectors(fields)
        else:
            keys = set(self.Schema.fields.keys())

        self.internal.state.mark(keys)
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

    def copy(self) -> 'Resource':
        """
        Create a clone of this Resource
        """
        clone = type(self)(state=deepcopy(self.internal.state))
        return clone.clean()

    def load(self, resolvers: Set[Text] = None) -> 'Resource':
        if self._id is None:
            return self

        if isinstance(resolvers, str):
            resolvers = {resolvers}

        # TODO: fix up Query so that even if the fresh object does exist in the
        # DAL, it will still try to execute the resolvers on the uncreated
        # object.

        # resolve a fresh copy throught the DAL and merge state
        # into this Resource.
        query = self.select(resolvers).where(_id=self._id)
        fresh = query.execute(first=True)
        if fresh:
            self.merge(fresh)
            self.clean(fresh.internal.state.keys())

        return self

    def reload(self, resolvers: Set[Text] = None) -> 'Resource':
        if isinstance(resolvers, str):
            resolvers = {resolvers}
        loading_resolvers = {k for k in resolvers if self.is_loaded(k)}
        return self.load(loading_resolvers)

    def unload(self, resolvers: Set[Text] = None) -> 'Resource':
        """
        Remove the given keys from field data and/or relationship data.
        """
        if resolvers:
            if isinstance(resolvers, str):
                resolvers = {resolvers}
                keys = self._normalize_selectors(resolvers)
        else:
            keys = set(
                self.internal.state.keys() |
                self.ravel.resolvers.keys()
            )
        for k in keys:
            if k in self.internal.state:
                del self.internal.state[k]
            elif k in self.ravel.resolvers:
                del self.ravel.resolvers[k]

    def is_loaded(self, resolvers: Union[Text, Set[Text]]) -> bool:
        """
        Are all given field and/or relationship values loaded?
        """
        if resolvers:
            if isinstance(resolvers, str):
                resolvers = {resolvers}
                keys = self._normalize_selectors(resolvers)
        else:
            keys = set(
                self.internal.state.keys() |
                self.ravel.resolvers.keys()
            )

        for k in keys:
            is_key_in_data = k in self.internal.state
            is_key_in_resolvers = k in self.ravel.resolvers
            if not (is_key_in_data or is_key_in_resolvers):
                return False

        return True

    def _prepare_record_for_create(self):
        """
        Prepares a a Resource state dict for insertion via DAL.
        """
        # extract only those elements of state data that correspond to
        # Fields declared on this Resource class.
        if ID not in self.internal.state:
            self._id = self.store.create_id(record)

        # when inserting or updating, we don't want to write the _rev value on
        # accident. The DAL is solely responsible for modifying this value.
        if REV in self.internal.state:
            del self.internal.state[REV]

        record = {}
        for k, v in self.internal.state.items():
            resolver = self.ravel.resolvers.fields.get(k)
            if resolver is not None:
                if v is None and (not resolver.nullable):
                    gen_default_value = self.ravel.defaults.get(k)
                    if gen_default_value is not None:
                        record[k] = gen_default_value()
                    else:
                        raise ValidationError(
                            f'{resolver.name} is not nullable'
                        )
                else:
                    record[k] = v

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

    # CRUD Methods

    @classmethod
    def select(cls, *resolvers: Tuple[Text], parent: 'Query' = None):
        return Query(target=cls, parent=parent).select(resolvers)

    def create(self, data: Dict = None, **more_data) -> 'Resource':
        data = dict(data or {}, **more_data)
        if data:
            self.merge(data)

        prepared_record = self._prepare_record_for_create()
        prepared_record.pop(REV, None)

        created_record = self.store.dispatch('create', (prepared_record, ))

        self.internal.state.update(created_record)
        return self.clean()

    @classmethod
    def get(cls, _id, select=None) -> Union['Resource', 'Batch']:
        if _id is None:
            return None

        if is_sequence(_id):
            return self.get_many(_ids=_id, select=select)

        if not select:
            select = set(cls.ravel.schema.fields.keys())
        elif not isinstance(select, set):
            select = set(select)

        select |= {ID, REV}

        state = cls.ravel.store.fetch(_id, fields=select)
        return cls(state=state).clean() if state else None

    @classmethod
    def get_many(
        cls,
        _ids: List = None,
        select=None,
        offset=None,
        limit=None,
        order_by=None,
    ) -> 'Batch':
        """
        Return a list of Resources in the store.
        """
        if not _ids:
            return cls.Batch()

        if not select:
            select = set(cls.ravel.schema.fields)
        elif isinstance(select, set):
            select = set(select)

        select |= {ID, REV}

        if not (offset or limit or order_by):
            store = cls.ravel.store
            args = (_ids, )
            kwargs = {'fields': select}
            states = store.dispatch('fetch_many', args, kwargs).values()
            return cls.Batch(
                cls(state=state).clean() for state in states
                if state is not None
            )
        else:
            query = cls.select(select).where(cls._id.including(_ids))
            query = query.order_by(order_by).offset(offset).limit(limit)
            return query.execute()

    @classmethod
    def get_all(
        cls,
        select: Set[Text] = None,
        offset: int = None,
        limit: int = None,
    ) -> 'Batch':
        """
        Return a list of all Resources in the store.
        """
        return cls.query(
            select=select,
            where=cls._id != None,
            order_by=cls._id.asc,
            offset=offset,
            limit=limit,
        )

    def delete(self) -> 'Resource':
        """
        Call delete on this object's store and therefore mark all fields as dirty
        and delete its _id so that save now triggers Store.create.
        """
        self.store.dispatch('delete', (self._id, ))
        self.mark(self.internal.state.keys())
        self._id = None
        self._rev = None
        return self

    @classmethod
    def delete_many(cls, resources: List['Resource']) -> None:
        # extract ID's of all objects to delete and clear
        # them from the instance objects' state dicts
        resource_ids = []
        for resource in resources:
            resource.mark()
            resource_ids.append(resource._id)
            resource._id = None
            resource._rev = None

        # delete the records in the DAL
        store = cls.ravel.store
        store.dispatch('delete_many', args=(resource_ids, ))

    @classmethod
    def delete_all(cls) -> None:
        store = cls.ravel.store
        store.dispatch('delete_all')

    @classmethod
    def exists(cls, entity: 'Entity') -> bool:
        """
        Does a simple check if a Resource exists by id.
        """
        store = cls.ravel.store

        if not entity:
            return False

        if is_resource(entity):
            args = (entity._id, )
        else:
            id_value, errors = cls._id.resolver.field.process(entity)
            args = (id_value, )
            if errors:
                raise ValueError(str(errors))

        return store.dispatch('exists', args=args)


    @classmethod
    def exists_many(cls, entity: 'Entity') -> bool:
        """
        Does a simple check if a Resource exists by id.
        """
        store = cls.ravel.store

        if not entity:
            return False

        if is_batch(entity):
            args = (entity._id, )
        else:
            assert is_sequence(entity)
            id_list = entity
            args = (id_list, )
            for id_value in id_list:
                value, errors = cls._id.resolver.field.process(id_value)
                if errors:
                    raise ValueError(str(errors))

        return store.dispatch('exists_many', args=args)

    def save(self, depth=0):
        return self.save_many([self], depth=depth)[0]

    def create(self, data: Dict = None) -> 'Resource':
        if data:
            self.merge(data)

        prepared_record = self._prepare_record_for_create()
        prepared_record.pop(REV, None)

        created_record = self.store.dispatch('create', (prepared_record, ))

        self.internal.state.update(created_record)
        return self.clean()

    def update(self, data: Dict = None, **more_data) -> 'Resource':
        data = dict(data or {}, **more_data)
        if data:
            self.merge(data)

        raw_record = self.dirty.copy()
        raw_record.pop(REV, None)
        raw_record.pop(ID, None)

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
                    ID: self._id,
                    'errors': errors,
                }
            )

        updated_record = self.store.dispatch(
            'update', (self._id, prepared_record)
        )

        self.internal.state.update(updated_record)
        return self.clean()

    @classmethod
    def create_many(cls, resources: List['Resource']) -> 'Batch':
        """
        Call `store.create_method` on input `Resource` list and return them in
        the form of a Batch.
        """
        records = []

        for resource in resources:
            if resource is None:
                continue
            if isinstance(resource, dict):
                state_dict = resource
                resource = cls(state=state_dict)

            record = resource._prepare_record_for_create()
            records.append(record)

        store = cls.ravel.store
        created_records = store.dispatch('create_many', (records, ))

        for resource, record in zip(resources, created_records):
            resource.internal.state.update(record)
            resource.clean()

        return cls.Batch(resources)

    @classmethod
    def update_many(
        cls, resources: List['Resource'], data: Dict = None, **more_data
    ) -> 'Batch':
        """
        Call the Store's update_many method on the list of Resources. Multiple
        Store calls may be made. As a preprocessing step, the input resource list
        is partitioned into groups, according to which subset of fields are
        dirty.

        For example, consider this list of resources,

        ```python
        resources = [
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

        # in the procedure below, we partition all incoming Resources
        # into groups, grouped by the set of fields being updated. In this way,
        # we issue an update_many datament for each partition in the DAL.
        partitions = defaultdict(list)

        for resource in resources:
            if resource is None:
                continue
            if common_values:
                resource.merge(common_values)

            partition_key = tuple(resource.dirty.keys())
            partitions[partition_key].append(resource)

        for resource_partition in partitions.values():
            records, _ids = [], []

            for resource in resource_partition:
                record = resource.dirty.copy()
                record.pop(REV, None)
                record.pop(ID, None)
                records.append(record)
                _ids.append(resource._id)

            store = cls.ravel.store
            updated_records = store.dispatch('update_many', (_ids, records))

            for resource in resource_partition:
                record = updated_records.get(resource._id)
                if record:
                    resource.internal.state.update(record)
                    resource.clean()

        return cls.Batch(resources)

    @classmethod
    def save_many(
        cls,
        resources: List['Resource'],
        depth: int = 0
    ) -> 'Batch':
        """
        Essentially a bulk upsert.
        """
        def seems_created(resource):
            return (
                (ID in resource.internal.state) and
                (ID not in resource.internal.state.dirty)
            )

        # partition resources into those that are "uncreated" and those which
        # simply need to be updated.
        to_update = []
        to_create = []
        for resource in resources:
            # TODO: merge duplicates
            if not seems_created(resource):
                to_create.append(resource)
            else:
                to_update.append(resource)

        # perform bulk create and update
        if to_create:
            created = cls.create_many(to_create)
        if to_update:
            updated = cls.update_many(to_update)

        retval = cls.Batch(to_update + to_create)

        if depth < 1:
            # base case. do not recurse on Resolvers
            return retval

        # aggregate and save all Resources referenced by all objects in
        # `resource` via their resolvers.
        class_2_objects = defaultdict(set)
        resolvers = cls.ravel.resolvers.by_tag('fields', invert=True)
        for resolver in resolvers.values():
            for resource in resources:
                if resolver.name in resource.internal.state:
                    value = resource.internal.state[resolver.name]
                    entity_to_save = resolver.on_save(resolver, resource, value)
                    if entity_to_save:
                        if is_resource(entity_to_save):
                            class_2_objects[resolver.owner].add(
                                entity_to_save
                            )
                        else:
                            assert is_sequence(entity_to_save)
                            class_2_objects[resolver.owner].update(
                                entity_to_save
                            )

        # recursively call save_many for each type of Resource
        for resource_type, resources in class_2_objects.items():
            resource_type.save_many(resources, depth=depth-1)

        return retval
