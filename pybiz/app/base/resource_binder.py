from typing import Dict, List, Type, Set, Text

from pybiz.util.misc_functions import is_sequence, get_class_name
from pybiz.util.loggers import console

# XXX: This stuff needs some refactoring. Too hacky.

class BizBinding(object):
    def __init__(self, binder, biz_class, store_instance, store_bind_kwargs=None):
        self.binder = binder
        self.biz_class = biz_class
        self.store_instance = store_instance
        self.store_bind_kwargs = store_bind_kwargs or {}
        self._is_bound = False

    def __repr__(self):
        return (
            f'BizBinding({self.biz_class_name}, {self.store_class_name}, '
            f'bound={self.is_bound})'
        )

    def bind(self, binder=None):
        binder = binder or self.binder

        # associate a singleton Store instance with the biz class.
        self.store_instance.bind(self.biz_class, **self.store_bind_kwargs)

        # first call bind on the Resource class itself
        self.biz_class.bind(binder)

        self._is_bound = True

    @property
    def is_bound(self):
        return self._is_bound

    @property
    def store_class(self):
        return self.store_instance.__class__

    @property
    def store_class_name(self):
        return get_class_name(self)

    @property
    def biz_class_name(self):
        return get_class_name(self.biz_class)


class ResourceBinder(object):
    """
    Stores and manages a global app, entailing which Resource class is
    associated with which Store class.
    """

    def __init__(self):
        self._bindings = {}
        self._named_store_classes = {}
        self._named_biz_classes = {}

    def __repr__(self):
        return f'{get_class_name(self)}()'

    @property
    def bindings(self) -> List['BizBinding']:
        return list(self._bindings.values())

    @property
    def biz_classes(self) -> Dict[Text, 'Resource']:
        return self._named_biz_classes

    @property
    def store_classes(self) -> Dict[Text, 'Store']:
        return self._named_store_classes

    def get_binding(self, biz_class):
        if isinstance(biz_class, type):
            biz_class = get_class_name(biz_class)
        return self._bindings.get(biz_class)

    def register(
        self,
        biz_class: Type['Resource'],
        store_class: Type['Store'],
        store_instance: 'Store' = None,
        store_bind_kwargs: Dict = None,
    ):
        store_class_name = get_class_name(store_class)
        if store_class_name not in self._named_store_classes:
            store_class = type(store_class_name, (store_class, ), {})
            self._named_store_classes[store_class_name] = store_class

            console.debug(
                f'registered Store "{store_class_name}" '
                f'with {get_class_name(self)}'
            )

        if store_instance is not None:
            assert isinstance(store_instance, store_class)
        else:
            store_instance = store_class()

        if biz_class is not None:
            biz_class_name = get_class_name(biz_class)
            biz_class.binder = self

            self._named_biz_classes[biz_class_name] = biz_class
            self._bindings[biz_class_name] = binding = BizBinding(
                self,
                biz_class=biz_class,
                store_instance=store_instance,
                store_bind_kwargs=store_bind_kwargs,
            )

            console.debug(
                f'registered Resource "{biz_class_name}" '
                f'with {get_class_name(self)}'
            )
            return binding

        return None

    def bind(self, biz_classes: Set[Type['Resource']] = None, rebind=False):
        if not biz_classes:
            biz_classes = [v.biz_class for v in self._bindings.values()]
        elif not is_sequence(biz_classes):
            biz_classes = [biz_classes]
        for biz_class in biz_classes:
            if not biz_class.pybiz.is_abstract:
                biz_class.binder = self
                self.get_store_instance(biz_class, rebind=rebind)

    def get_store_instance(
        self,
        biz_class: Type['Resource'],
        bind=True,
        rebind=False,
    ) -> 'Store':
        if isinstance(biz_class, str):
            binding = self._bindings.get(biz_class)
        else:
            binding = self._bindings.get(get_class_name(biz_class))

        if binding is None:
            # lazily register a new binding
            base_store_class = biz_class.__store__()
            console.debug(
                f'calling {get_class_name(biz_class)}.__store__()'
            )
            binding = self.register(biz_class, base_store_class)

        # call bind only if it already hasn't been called
        if rebind or ((not binding.is_bound) and bind):
            console.debug(
                message=(
                    f'binding "{get_class_name(binding.store_instance)}" '
                    f'with "{get_class_name(binding.biz_class)}"'
                )
            )
            binding.bind(binder=self)

        return binding.store_instance

    def get_store_class(self, store_class_name: Text) -> Type['Store']:
        return self._named_store_classes.get(store_class_name)

    def is_registered(self, biz_class: Type['Resource']) -> bool:
        if isinstance(biz_class, str):
            return biz_class in self._bindings
        else:
            return get_class_name(biz_class) in self._bindings

    def is_bound(self, biz_class: Type['Resource']) -> bool:
        if isinstance(biz_class, str):
            return self._bindings[biz_class].is_bound
        else:
            return self._bindings[get_class_name(biz_class)].is_bound
