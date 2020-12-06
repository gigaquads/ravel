from typing import Dict, List, Type, Set, Text

from ravel.util.misc_functions import is_sequence, get_class_name
from ravel.util.loggers import console

# XXX: This stuff needs some refactoring. Too hacky.


class BizBinding(object):

    def __init__(self, binder, resource_type, store_instance, store_bind_kwargs=None):
        self.binder = binder
        self.resource_type = resource_type
        self.store_instance = store_instance
        self.store_bind_kwargs = store_bind_kwargs or {}
        self._is_bound = False

    def __repr__(self):
        return (
            f'BizBinding({self.resource_type_name}, {self.store_class_name}, '
            f'bound={self.is_bound})'
        )

    def bind(self, binder=None, force=False):
        if self._is_bound:
            if not force:
                console.warning(
                    message=(
                        'resource store binding already set. '
                        'skipping repeated bind call'
                    ),
                    data={'resource_type': get_class_name(self.resource_type)}
                )
                return

        binder = binder or self.binder

        # associate a singleton Store instance with the res class.
        self.store_instance.bind(self.resource_type, **self.store_bind_kwargs)

        # first call bind on the Resource class itself
        self.resource_type.bind(binder)

        self._is_bound = True

    @property
    def is_bound(self):
        return self._is_bound

    @property
    def store_type(self):
        return self.store_instance.__class__

    @property
    def store_class_name(self):
        return get_class_name(self)

    @property
    def resource_type_name(self):
        return get_class_name(self.resource_type)


class ResourceBinder(object):
    """
    Stores and manages a global app, entailing which Resource class is
    associated with which Store class.
    """

    def __init__(self):
        self._bindings = {}
        self._named_store_types = {}
        self._named_resource_types = {}

    def __repr__(self):
        return f'{get_class_name(self)}()'

    @property
    def bindings(self) -> List['BizBinding']:
        return list(self._bindings.values())

    @property
    def resource_types(self) -> Dict[Text, 'Resource']:
        return self._named_resource_types

    @property
    def store_types(self) -> Dict[Text, 'Store']:
        return self._named_store_types

    def get_binding(self, resource_type):
        if isinstance(resource_type, type):
            resource_type = get_class_name(resource_type)
        return self._bindings.get(resource_type)

    def register(
        self,
        resource_type: Type['Resource'],
        store_type: Type['Store'],
        store_instance: 'Store' = None,
        store_bind_kwargs: Dict = None,
    ):
        store_class_name = get_class_name(store_type)
        if store_class_name not in self._named_store_types:
            store_type = type(store_class_name, (store_type, ), {})
            self._named_store_types[store_class_name] = store_type

        if store_instance is not None:
            assert isinstance(store_instance, store_type)
        else:
            store_instance = store_type()

        if resource_type is not None:
            resource_type_name = get_class_name(resource_type)
            resource_type.binder = self
            self._named_resource_types[resource_type_name] = resource_type
            self._bindings[resource_type_name] = binding = BizBinding(
                self,
                resource_type=resource_type,
                store_instance=store_instance,
                store_bind_kwargs=store_bind_kwargs,
            )
            return binding

        return None

    def bind(self, resource_types: Set[Type['Resource']] = None, rebind=False):
        if not resource_types:
            resource_types = [v.resource_type for v in self._bindings.values()]
        elif not is_sequence(resource_types):
            resource_types = [resource_types]
        for resource_type in resource_types:
            if not resource_type.ravel.is_abstract:
                resource_type.binder = self
                self.get_store_instance(resource_type, rebind=rebind)

    def get_store_instance(
        self,
        resource_type: Type['Resource'],
        bind=True,
        rebind=False,
    ) -> 'Store':
        if isinstance(resource_type, str):
            binding = self._bindings.get(resource_type)
        else:
            binding = self._bindings.get(get_class_name(resource_type))

        if binding is None:
            # lazily register a new binding
            base_store_type = resource_type.__store__()
            console.debug(
                f'calling {get_class_name(resource_type)}.__store__()'
            )
            binding = self.register(resource_type, base_store_type)

        # call bind only if it already hasn't been called
        if rebind or ((not binding.is_bound) and bind):
            console.debug(
                message=(
                    f'setting {get_class_name(binding.resource_type)}.ravel.store '
                    f'= {get_class_name(binding.store_instance)}()'
                )
            )
            binding.bind(binder=self)

        return binding.store_instance

    def get_store_type(self, store_class_name: Text) -> Type['Store']:
        return self._named_store_types.get(store_class_name)

    def is_registered(self, resource_type: Type['Resource']) -> bool:
        if isinstance(resource_type, str):
            return resource_type in self._bindings
        else:
            return get_class_name(resource_type) in self._bindings

    def is_bound(self, resource_type: Type['Resource']) -> bool:
        if isinstance(resource_type, str):
            return self._bindings[resource_type].is_bound
        else:
            return self._bindings[get_class_name(resource_type)].is_bound
