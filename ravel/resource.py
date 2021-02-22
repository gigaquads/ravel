import inspect
import uuid

from typing import Text, Tuple, List, Set, Dict, Type, Union, Optional
from collections import defaultdict
from datetime import datetime
from copy import deepcopy
from threading import local

from appyratus.utils.dict_utils import DictObject
from appyratus.utils.type_utils import TypeUtils

from ravel.exceptions import NotBootstrapped, ValidationError
from ravel.store import Store, SimulationStore
from ravel.util.dirty import DirtyDict
from ravel.util.loggers import console
from ravel.util import (
    is_batch,
    is_resource,
    is_resource_type,
    is_sequence,
    get_class_name,
)
from ravel.schema import Field, Schema, Id, fields
from ravel.dumper import Dumper, SideLoadedDumper, DumpStyle
from ravel.batch import Batch
from ravel.constants import (
    IS_RESOURCE,
    ID,
    REV,
)

from ravel.query.query import Query
from ravel.query.predicate import PredicateParser
from ravel.resolver.resolver import Resolver
from ravel.resolver.decorators import field as field_decorator
from ravel.resolver.resolver_decorator import ResolverDecorator
from ravel.resolver.resolver_property import ResolverProperty
from ravel.resolver.resolver_manager import ResolverManager
from ravel.resolver.resolvers.loader import Loader
from ravel.entity import Entity


class ResourceMeta(type):
    def __init__(cls, name, bases, dct):
        declared_fields = cls._initialize_class_state()
        cls._inherit_resolvers(bases)
        cls._build_schema_type(declared_fields, bases)
        cls.Batch = Batch.factory(cls)

    def _inherit_resolvers(cls, base_classes):
        for base_class in base_classes:
            if is_resource_type(base_class):
                for key, resolver in base_class.ravel.resolvers.items():
                    copied_resolver = resolver.copy(new_owner=cls)
                    resolver_property = ResolverProperty(copied_resolver)
                    cls.ravel.resolvers.register(copied_resolver)
                    setattr(cls, key, resolver_property)

    def _initialize_class_state(resource_type):
        setattr(resource_type, IS_RESOURCE, True)

        resource_type.ravel = DictObject()
        resource_type.ravel.local = local()
        resource_type.ravel.app = None
        resource_type.ravel.resolvers = ResolverManager()
        resource_type.ravel.foreign_keys = {}
        resource_type.ravel.is_abstract = resource_type._compute_is_abstract()
        resource_type.ravel.is_bootstrapped = False
        resource_type.ravel.is_bound = False
        resource_type.ravel.schema = None
        resource_type.ravel.defaults = {}
        resource_type.ravel.protected_field_names = set()
        resource_type.ravel.predicate_parser = PredicateParser(resource_type)
        resource_type.ravel.virtual_fields = {}

        return resource_type._process_fields()

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
                resolver_type = field.meta.get('resolver_type') or Loader
                resolver_property = resolver_type.build_property(
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

        fields = dict(inherited_fields, **fields)

        # perform final processing now that we have all direct and
        # inherited fields in one dict.
        default_protected = resource_type.__protected__()
        for k, field in fields.items():
            if field.meta.get('ravel_on_resolve'):
                resource_type.ravel.virtual_fields[k] = field
            if field.meta.get('protected', default_protected) or k in {REV}:
                resource_type.ravel.protected_field_names.add(k)
            if k in inherited_fields:
                resolver_type = field.meta.get('resolver_type') or Loader
                resolver_property = resolver_type.build_property(kwargs=dict(
                    owner=resource_type,
                    field=field,
                    name=k,
                    target=resource_type,
                ))
                resource_type.ravel.resolvers.register(
                    resolver_property.resolver
                )
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
        resource_type.ravel.defaults = (
            resource_type._extract_field_defaults(schema)
        )

    def _copy_fields_from_mixin(resource_type, class_obj):
        fields = {}
        is_field = lambda x: isinstance(x, Field)
        for k, field in inspect.getmembers(class_obj, predicate=is_field):
            if k == 'Schema':
                continue
            fields[k] = deepcopy(field)
        return fields

    def _extract_field_defaults(resource_type, schema):

        def make_callable_default(field):
            value = field.default

            def default_callable():
                return deepcopy(value)

            default_callable.__name__ = f'{field.name}_default'
            return default_callable

        defaults = resource_type.ravel.defaults

        for field in schema.fields.values():
            if field.default is not None:
                # move field default into "defaults" dict
                if callable(field.default):
                    defaults[field.name] = field.default
                else:
                    defaults[field.name] = make_callable_default(field)
                # clear it from the schema object once "defaults" dict
                field.default = None
        return defaults


class Resource(Entity, metaclass=ResourceMeta):

    # metaclass attribute stubs:
    ravel = None

    # base resource fields:
    _id = fields.UuidString(default=lambda: uuid.uuid4().hex, nullable=False)
    _rev = fields.String()

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
        elif isinstance(id_value, uuid.UUID):
            id_str = id_value.hex[:7]
        else:
            id_str = repr(id_value)

        return f'{name}({id_str}){dirty}'

    @classmethod
    def __abstract__(cls) -> bool:
        return True

    @classmethod
    def __protected__(cls) -> bool:
        return False

    @classmethod
    def __store__(cls) -> Type[Store]:
        return SimulationStore

    @classmethod
    def on_bootstrap(cls, app, **kwargs):
        pass

    @classmethod
    def on_bind(cls):
        pass

    @classmethod
    def bootstrap(cls, app: 'Application', **kwargs):
        t1 = datetime.now()
        cls.ravel.app = app

        # bootstrap all resolvers owned by this class
        for resolver in cls.ravel.resolvers.values():
            if not resolver.is_bootstrapped():
                resolver.bootstrap(cls.ravel.app)

        # lastly perform custom developer logic
        cls.on_bootstrap(app, **kwargs)
        cls.ravel.local.is_bootstrapped = True

        t2 = datetime.now()
        secs = (t2 - t1).total_seconds()
        console.debug(
            f'bootstrapped {TypeUtils.get_class_name(cls)} '
            f'in {secs:.2f}s'
        )


    @classmethod
    def bind(cls, store: 'Store', **kwargs):
        t1 = datetime.now()
        cls.ravel.local.store = store

        for resolver in cls.ravel.resolvers.values():
            resolver.bind()

        cls.on_bind()
        cls.ravel.is_bound = True

        t2 = datetime.now()
        secs = (t2 - t1).total_seconds()
        console.debug(
            f'bound {TypeUtils.get_class_name(store)} to '
            f'{TypeUtils.get_class_name(cls)} '
            f'in {secs:.2f}s'
        )

    @classmethod
    def is_bootstrapped(cls) -> bool:
        return cls.ravel.local.is_bootstrapped

    @classmethod
    def is_bound(cls) -> bool:
        return cls.ravel.is_bound

    @property
    def app(self) -> 'Application':
        if not self.ravel.app:
            raise NotBootstrapped(
                f'{get_class_name(self)} must be associated '
                f'with a bootstrapped app'
            )
        return self.ravel.app

    @property
    def log(self) -> 'ConsoleLoggerInterface':
        return self.ravel.app.log if self.ravel.app else None

    @property
    def class_name(self) -> Text:
        return get_class_name(self)

    @property
    def store(self) -> 'Store':
        return self.ravel.local.store

    @property
    def is_dirty(self) -> bool:
        return bool(
            self.internal.state.dirty &
            self.ravel.schema.fields.keys()
        )

    @property
    def dirty(self) -> Dict:
        return {
            k: self.internal.state[k]
            for k in self.internal.state.dirty
            if k in self.Schema.fields
        }

    @classmethod
    def generate(
        cls,
        keys: Set[Text] = None,
        values: Dict = None,
        use_defaults=True,
    ) -> 'Resource':
        instance = cls()
        values = values or {}
        keys = keys or set(cls.ravel.schema.fields.keys())
        resolver_objs = Resolver.sort([
            cls.ravel.resolvers[k] for k in keys
            if k not in {REV}
        ])

        instance = cls()

        for resolver in resolver_objs:
            if resolver.name in values:
                value = values[resolver.name]
            elif use_defaults and resolver.name in cls.ravel.defaults:
                func = cls.ravel.defaults[resolver.name]
                value = func()
            else:
                value = resolver.generate(instance)

            instance.internal.state[resolver.name] = value

        return instance

    def set(self, other=None, **values) -> 'Resource':
        return self.merge(other=other, **values)

    def merge(self, other=None, **values) -> 'Resource':
        try:
            if isinstance(other, dict):
                for k, v in other.items():
                    setattr(self, k, v)
            elif isinstance(other, Resource):
                for k, v in other.internal.state.items():
                    setattr(self, k, v)

            if values:
                self.merge(values)

            return self
        except Exception:
            self.app.log.error(
                message=(
                    f'failed to merge object into '
                    f'resource {str(self._id)[:7]}'
                ),
                data={
                    'resource': self._id,
                    'class': self.class_name,
                    'other': other,
                    'values': values,
                }
            )
            raise

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

    def dump(
        self,
        resolvers: Optional[Set[Text]] = None,
        style: Optional[DumpStyle] = None
    ) -> Dict:
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

    def validate(self, resolvers: Set[Text] = None, strict=False) -> Dict:
        """
        Validate an object's loaded state data. If you need to check if some
        state data is loaded or not and raise an exception in case absent,
        use self.require.
        """
        errors = {}
        resolver_names_to_validate = (
            resolvers or set(self.ravel.resolvers.keys())
        )
        for name in resolver_names_to_validate:
            resolver = self.ravel.resolvers[name]
            if name not in self.internal.state:
                console.warning(
                    message=f'skipping {name} validation',
                    data={'reason': 'not loaded'}
                )
            else:
                value = self.internal.state.get(name)
                if value is None and not resolver.nullable:
                    errors[name] = 'not nullable'
                if name in self.ravel.schema.fields:
                    field = self.ravel.schema.fields[name]
                    _value, error = field.process(value)
                    if error is not None:
                        errors[name] = error

        if strict and errors:
            console.error(
                message='validation error',
                data={'errors': errors}
            )
            raise ValidationError('see error log for details')

        return errors

    def require(
        self,
        resolvers: Set[Text] = None,
        use_defaults=True,
        strict=False,
        overwrite=False,
    ) -> Set[Text]:
        """
        Checks if all specified resolvers are present. If they are required
        but not present, an exception will be raised for `strict` mode;
        otherwise, a set of the missing resolver names is returned.
        """
        if isinstance(resolvers, str):
            resolvers = {resolvers}
        required_resolver_names = (
            resolvers or set(
                k for k in self.ravel.resolvers.keys()
                if self.ravel.resolvers[k].required
            )
        )
        missing_resolver_names = set()
        defaults = self.ravel.defaults
        for name in required_resolver_names:
            resolver = self.ravel.resolvers[name]
            if overwrite or name not in self.internal.state:
                if use_defaults and name in defaults:
                    value = defaults[name]()
                    if value is None and not resolver.nullable:
                        raise ValidationError(f'{name} not nullable')
                    self[name] = value
                else:
                    missing_resolver_names.add(name)

        if strict and missing_resolver_names:
            console.error(
                message=f'{self.class_name} missing required data',
                data={'missing': missing_resolver_names}
            )
            raise ValidationError('see error log for details')

        return missing_resolver_names

    def resolve(self, resolvers: Union[Text, Set[Text]] = None) -> 'Resource':
        """
        Execute each of the resolvers, specified by name, storing the results
        in `self.internal.state`.
        """
        if self._id is None:
            return self

        if isinstance(resolvers, str):
            resolvers = {resolvers}
        elif not resolvers:
            resolvers = set(self.ravel.resolvers.keys())

        # execute all requested resolvers
        for k in resolvers:
            resolver = self.ravel.resolvers.get(k)
            if resolver is not None:
                if k in self.ravel.resolvers.fields:
                    # field loader resolvers are treated specially to overcome
                    # the limitation of Resolver.target always expecte to be a
                    # Resource class.
                    # if resolvers == {'session_state'}:
                    #     print(resolvers)
                    #     import ipdb; ipdb.set_trace()
                    obj = resolver.resolve(self)
                    setattr(self, k, getattr(obj, k))
                else:
                    setattr(self, k, resolver.resolve(self))

        # clean the resolved values so they arent't accidently saved on
        # update/create, as we just fetched them from the store.
        self.clean(resolvers)

        return self

    def refresh(self, keys: Union[Text, Set[Text]] = None) -> 'Resource':
        """
        For any field currently loaded in the resource, fetch a fresh copy
        from the store.
        """
        if not keys:
            keys = set(self.internal.state.keys())
        elif isinstance(keys, str):
            keys = {keys}

        return self.resolve(keys)

    def unload(self, keys: Set[Text] = None) -> 'Resource':
        """
        Remove the given keys from field data and/or relationship data.
        """
        if keys:
            if isinstance(keys, str):
                keys = {keys}
                keys = self._normalize_selectors(keys)
        else:
            keys = set(
                self.internal.state.keys() |
                self.ravel.resolvers.keys()
            )
        for k in keys:
            if k in self.internal.state:
                del self.internal.state[k]

        return self

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

    def _prepare_record_for_create(self, keys_to_save: Optional[Set[Text]] = None):
        """
        Prepares a a Resource state dict for insertion via DAL.
        """
        # extract only those elements of state data that correspond to
        # Fields declared on this Resource class.
        if ID not in self.internal.state:
            self._id = self.ravel.local.store.create_id(self.internal.state)

        # when inserting or updating, we don't want to write the _rev value on
        # accident. The DAL is solely responsible for modifying this value.
        if REV in self.internal.state:
            del self.internal.state[REV]

        record = {}
        keys_to_save = keys_to_save or set(self.internal.state.keys())
        keys_to_save |= self.ravel.defaults.keys()
        keys_to_save &= self.ravel.resolvers.fields.keys()

        for key in keys_to_save:
            resolver = self.ravel.resolvers[key]
            default = self.ravel.defaults.get(key)
            if key not in self.internal.state:
                if default is not None:
                    self.internal.state[key] = value = default()
                    record[key] = value
                elif resolver.required:
                    raise ValidationError(f'{key} is a required field')
            else:
                value = self.internal.state[key]
                if value is None and (not resolver.nullable):
                    if default:
                        self.internal.state[key] = value = default()
                    # if the value is still none, just remove it from
                    # the state dict instead of raising
                    if self.internal.state[key] is None:
                        console.warning(
                            message=(
                                f'resolved None for {resolver} but not '
                                f'nullable'
                            ),
                            data={
                                'resource': self._id,
                                'class': self.class_name,
                                'field': key,
                            }
                        )
                        del self.internal.state[key]
                        continue
                record[key] = self.internal.state[key]

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
    def select(
        cls,
        *resolvers: Tuple[Text],
        parent: 'Query' = None,
        request: 'Request' = None,
    ) -> 'Query':
        query = Query(target=cls, request=request, parent=parent)
        query.select(resolvers)
        cls.on_select(query)
        query.callbacks.append(cls.post_select)
        return query

    def create(self, data: Dict = None, **more_data) -> 'Resource':
        self.pre_create()

        data = dict(data or {}, **more_data)
        if data:
            self.merge(data)

        prepared_record = self._prepare_record_for_create()
        prepared_record.pop(REV, None)

        self.on_create(prepared_record)

        created_record = self.ravel.local.store.dispatch(
            'create', (prepared_record, )
        )

        self.merge(created_record)
        self.clean()
        self.post_create()

        return self

    def setdefault(self, key, value):
      if key in self.internal.state:
        return self[key]
      else:
        self[key] = value
        return value

    @classmethod
    def get(cls, _id, select=None) -> Optional[Union['Resource', 'Batch']]:
        if _id is None:
            return None

        if is_sequence(_id):
            return cls.get_many(_ids=_id, select=select)

        if not select:
            select = set(cls.ravel.schema.fields.keys())
        elif not isinstance(select, set):
            select = set(select)

        select |= {ID, REV}
        select -= cls.ravel.virtual_fields.keys()

        cls.on_get(_id, select)

        state = cls.ravel.local.store.dispatch(
            'fetch', (_id, ), {'fields': select}
        )

        resource = cls(state=state).clean() if state else None

        cls.post_get(resource)

        return resource

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
        select -= cls.ravel.virtual_fields.keys()

        if not (offset or limit or order_by):
            store = cls.ravel.local.store
            args = (_ids, )
            kwargs = {'fields': select}
            states = store.dispatch('fetch_many', args, kwargs).values()
            cls.on_get_many(_ids, select)
            batch = cls.Batch(
                cls(state=state).clean() for state in states
                if state is not None
            )
            cls.post_get_many(batch)
        else:
            query = cls.select(select).where(cls._id.including(_ids))
            query = query.order_by(order_by).offset(offset).limit(limit)
            cls.on_select(query)
            batch = query.execute()
            cls.post_select(query, batch)

        return batch

    @classmethod
    def get_all(
        cls,
        select: Set[Text] = None,
        order_by: List['OrderBy'] = None,
        offset: int = None,
        limit: int = None,
    ) -> 'Batch':
        """
        Return a list of all Resources in the store.
        """
        # build the query
        query = cls.select().where(cls._id != None)

        if select:
            query = query.select(select)
        if order_by:
            query = query.order_by(order_by)
        if limit and limit > 0:
            query = query.limit(limit)
        if offset is not None and offset >= 0:
            query = query.offset(offset)

        cls.on_select(query)

        batch = query.execute()

        cls.post_select(query, batch)

        return batch

    def delete(self) -> 'Resource':
        """
        Call delete on this object's store and therefore mark all fields as
        dirty and delete its _id so that save now triggers Store.create.
        """
        self.ravel.local.store.dispatch('delete', (self._id, ))
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

        if resource_ids:
            store = cls.ravel.local.store
            cls.on_delete_many(resource_ids)
            store.dispatch('delete_many', args=(resource_ids, ))
            cls.post_delete_many(resource_ids)

    @classmethod
    def delete_all(cls) -> None:
        store = cls.ravel.local.store
        store.dispatch('delete_all')

    @classmethod
    def exists(cls, entity: 'Entity') -> bool:
        """
        Does a simple check if a Resource exists by id.
        """
        store = cls.ravel.local.store

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
        store = cls.ravel.local.store

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

    def save(self, resolvers: Union[Text, Set[Text]] = None, depth=0) -> 'Resource':
        return self.save_many([self], resolvers=resolvers, depth=depth)[0]

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
                if field.name not in self.ravel.virtual_fields:
                    if v is None and field.nullable:
                        prepared_record[k] = None
                    else:
                        prepared_record[k], error = field.process(v)
                        if error:
                            console.error(
                                f'{self} failed validation for {k}: {v}'
                            )
                            errors[k] = error

        self.on_update(prepared_record)

        if errors:
            raise ValidationError(f'update failed for {self}: {errors}')

        updated_record = self.ravel.local.store.dispatch(
            'update', (self._id, prepared_record)
        )

        if updated_record:
            self.merge(updated_record)

        self.clean(prepared_record.keys() | updated_record.keys())
        self.post_update()

        return self

    @classmethod
    def create_many(
        cls,
        resources: List['Resource'],
        fields: Set[Text] = None
    ) -> 'Batch':
        """
        Call `store.create_method` on input `Resource` list and return them in
        the form of a Batch.
        """
        # normalize resources to a list of Resource objects
        records = []
        prepared_resources = []
        for resource in resources:
            if resource is None:
                continue
            if isinstance(resource, dict):
                # convert raw dict into a proper Resource object
                state_dict = resource
                resource = cls(state=state_dict)

            record = resource._prepare_record_for_create(fields)
            records.append(record)

            resource.internal.state.update(record)
            prepared_resources.append(resource)

        if not records:
            return cls.Batch()

        cls.on_create_many(records)

        store = cls.ravel.local.store
        created_records = store.dispatch('create_many', (records, ))

        for resource, record in zip(prepared_resources, created_records):
            resource.merge(record)
            resource.clean()

        # insert the batch to the store
        batch = cls.Batch(prepared_resources)
        cls.post_create_many(batch)

        return batch

    @classmethod
    def update_many(
        cls,
        resources: List['Resource'],
        fields: Set[Text] = None,
        data: Dict = None,
        **more_data
    ) -> 'Batch':
        """
        Call the Store's update_many method on the list of Resources.
        Multiple Store calls may be made. As a preprocessing step, the input
        resource list is partitioned into groups, according to which subset
        of fields are dirty.

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

        A spearate store call to `update_many` will be made for each partition.
        """
        # common_values are values that should be updated
        # across all objects.
        common_values = dict(data or {}, **more_data)

        # in the procedure below, we partition all incoming Resources
        # into groups, grouped by the set of fields being updated. In this way,
        # we issue an update_many datament for each partition in the DAL.
        partitions = defaultdict(cls.Batch)

        fields_to_update = fields

        for resource in resources:
            if resource is None:
                continue
            if common_values:
                resource.merge(common_values)

            partition_key = tuple(resource.dirty.keys())
            partitions[partition_key].append(resource)

        # id_2_copies used to synchronize updated state across all
        # instances that share the same ID.
        id_2_copies = defaultdict(list)

        cls.on_update_many(cls.Batch(resources))

        for partition_key_tuple, resource_partition in partitions.items():
            records, _ids = [], []

            for resource in resource_partition:
                record = resource.dirty.copy()
                record.pop(REV, None)
                record.pop(ID, None)
                if fields_to_update:
                    record = {
                        k: v for k, v in record.items()
                        if (
                            (k in fields_to_update) and
                            (k not in cls.ravel.virtual_fields)
                        )
                    }
                records.append(record)
                _ids.append(resource._id)

            store = cls.ravel.local.store
            updated_records = store.dispatch('update_many', (_ids, records))

            if not updated_records:
                resource_partition.clean(partition_key_tuple)
                continue

            for resource in resource_partition:
                record = updated_records.get(resource._id)
                if record:
                    resource.merge(record)
                    resource.clean(record.keys())

                    # sync updated state across previously encoutered
                    # instances of this resource (according to ID)
                    if resource._id in id_2_copies:
                        for res_copy in id_2_copies[resource._id]:
                            res_copy.merge(record)
                    id_2_copies[resource._id].append(resource)

        updated_resources = cls.Batch(resources)

        cls.post_update_many(updated_resources)

        return updated_resources

    @classmethod
    def save_many(
        cls,
        resources: List['Resource'],
        resolvers: Union[Text, Set[Text]] = None,
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

        if resolvers is not None:
            if isinstance(resolvers, str):
                resolvers = {resolvers}
            elif not isinstance(resolvers, set):
                resolvers = set(resolvers)
            fields_to_save = set()
            resolvers_to_save = set()
            for k in resolvers:
                if k in cls.ravel.schema.fields:
                    fields_to_save.add(k)
                else:
                    resolvers_to_save.add(k)
        else:
            fields_to_save = None
            resolvers_to_save = set()

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
            cls.create_many(to_create, fields=fields_to_save)
        if to_update:
            cls.update_many(to_update, fields=fields_to_save)

        retval = cls.Batch(to_update + to_create)

        if depth < 1:
            # base case. do not recurse on Resolvers
            return retval

        # aggregate and save all Resources referenced by all objects in
        # `resource` via their resolvers.
        class_2_objects = defaultdict(set)
        resolvers = cls.ravel.resolvers.by_tag('fields', invert=True)
        for resolver in resolvers.values():
            if resolver.name not in resolvers_to_save:
                continue
            for resource in resources:
                if resolver.name in resource.internal.state:
                    value = resource.internal.state[resolver.name]
                    resolver.on_save(resolver, resource, value)
                    if value:
                        if is_resource(value):
                            class_2_objects[resolver.owner].add(value)
                        else:
                            assert is_sequence(value)
                            class_2_objects[resolver.owner].update(value)
                    elif value is None and not resolver.nullable:
                        raise ValidationError(
                            f'{get_class_name(cls)}.{resolver.name} '
                            f'is required by save'
                        )

        # recursively call save_many for each type of Resource
        for resource_type, resources in class_2_objects.items():
            resource_type.save_many(resources, depth=depth-1)

        return retval

    @classmethod
    def on_select(cls, query: 'Query'):
        pass

    @classmethod
    def post_select(cls, query: 'Query', result: 'Entity'):
        pass

    @classmethod
    def on_get(cls, _id, fields):
        pass

    @classmethod
    def on_get_many(cls, _ids, fields):
        pass

    @classmethod
    def post_get_many(cls, resources):
        pass

    @classmethod
    def post_get(cls, resource: 'Resource'):
        pass

    @classmethod
    def on_update_many(cls, batch):
        pass

    @classmethod
    def post_update_many(cls, batch):
        pass

    @classmethod
    def on_create_many(cls, batch):
        pass

    @classmethod
    def post_create_many(cls, batch):
        pass

    @classmethod
    def on_delete_many(cls, _ids):
        pass

    @classmethod
    def post_delete_many(cls, _ids):
        pass

    @classmethod
    def post_get(cls, resource: 'Resource'):
        pass

    def on_update(self, values: Dict):
        pass

    def post_update(self):
        pass

    def pre_create(self):
        pass

    def on_create(self, values: Dict):
        pass

    def post_create(self):
        pass

    def on_delete(self):
        pass

    def post_delete(self):
        pass