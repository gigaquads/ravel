from typing import Dict, List, Type, Set, Text

from pybiz.util.misc_functions import is_sequence, get_class_name
from pybiz.util.loggers import console

# XXX: This stuff needs some refactoring. Too hacky.

class BizBinding(object):
    def __init__(self, binder, biz_class, dao_instance, dao_bind_kwargs=None):
        self.binder = binder
        self.biz_class = biz_class
        self.dao_instance = dao_instance
        self.dao_bind_kwargs = dao_bind_kwargs or {}
        self._is_bound = False

    def __repr__(self):
        return (
            f'BizBinding({self.biz_class_name}, {self.dao_class_name}, '
            f'bound={self.is_bound})'
        )

    def bind(self, binder=None):
        binder = binder or self.binder

        # associate a singleton Dao instance with the biz class.
        self.dao_instance.bind(self.biz_class, **self.dao_bind_kwargs)

        # first call bind on the BizObject class itself
        self.biz_class.bind(binder)

        self._is_bound = True

    @property
    def is_bound(self):
        return self._is_bound

    @property
    def dao_class(self):
        return self.dao_instance.__class__

    @property
    def dao_class_name(self):
        return get_class_name(self)

    @property
    def biz_class_name(self):
        return get_class_name(self.biz_class)


class ApplicationDaoBinder(object):
    """
    Stores and manages a global app, entailing which BizObject class is
    associated with which Dao class.
    """

    def __init__(self):
        self._bindings = {}
        self._named_dao_classes = {}
        self._named_biz_classes = {}

    def __repr__(self):
        return f'{get_class_name(self)}()'

    @property
    def bindings(self) -> List['BizBinding']:
        return list(self._bindings.values())

    @property
    def biz_classes(self) -> Dict[Text, 'BizObject']:
        return self._named_biz_classes

    @property
    def dao_classes(self) -> Dict[Text, 'Dao']:
        return self._named_dao_classes

    def get_binding(self, biz_class):
        if isinstance(biz_class, type):
            biz_class = get_class_name(biz_class)
        return self._bindings.get(biz_class)

    def register(
        self,
        biz_class: Type['BizObject'],
        dao_class: Type['Dao'],
        dao_instance: 'Dao' = None,
        dao_bind_kwargs: Dict = None,
    ):
        dao_class_name = get_class_name(dao_class)
        if dao_class_name not in self._named_dao_classes:
            dao_class = type(dao_class_name, (dao_class, ), {})
            self._named_dao_classes[dao_class_name] = dao_class

            console.debug(
                f'registered Dao "{dao_class_name}" '
                f'with {get_class_name(self)}'
            )

        if dao_instance is not None:
            assert isinstance(dao_instance, dao_class)
        else:
            dao_instance = dao_class()

        if biz_class is not None:
            biz_class_name = get_class_name(biz_class)
            biz_class.binder = self

            self._named_biz_classes[biz_class_name] = biz_class
            self._bindings[biz_class_name] = binding = BizBinding(
                self,
                biz_class=biz_class,
                dao_instance=dao_instance,
                dao_bind_kwargs=dao_bind_kwargs,
            )

            console.debug(
                f'registered BizObject "{biz_class_name}" '
                f'with {get_class_name(self)}'
            )
            return binding

        return None

    def bind(self, biz_classes: Set[Type['BizObject']] = None, rebind=False):
        if not biz_classes:
            biz_classes = [v.biz_class for v in self._bindings.values()]
        elif not is_sequence(biz_classes):
            biz_classes = [biz_classes]
        for biz_class in biz_classes:
            if not biz_class.pybiz.is_abstract:
                biz_class.binder = self
                self.get_dao_instance(biz_class, rebind=rebind)

    def get_dao_instance(
        self,
        biz_class: Type['BizObject'],
        bind=True,
        rebind=False,
    ) -> 'Dao':
        if isinstance(biz_class, str):
            binding = self._bindings.get(biz_class)
        else:
            binding = self._bindings.get(get_class_name(biz_class))

        if binding is None:
            # lazily register a new binding
            base_dao_class = biz_class.__dao__()
            console.debug(
                f'calling {get_class_name(biz_class)}.__dao__()'
            )
            binding = self.register(biz_class, base_dao_class)

        # call bind only if it already hasn't been called
        if rebind or ((not binding.is_bound) and bind):
            console.debug(
                message=(
                    f'binding "{get_class_name(binding.dao_instance)}" '
                    f'with "{get_class_name(binding.biz_class)}"'
                )
            )
            binding.bind(binder=self)

        return binding.dao_instance

    def get_dao_class(self, dao_class_name: Text) -> Type['Dao']:
        return self._named_dao_classes.get(dao_class_name)

    def is_registered(self, biz_class: Type['BizObject']) -> bool:
        if isinstance(biz_class, str):
            return biz_class in self._bindings
        else:
            return get_class_name(biz_class) in self._bindings

    def is_bound(self, biz_class: Type['BizObject']) -> bool:
        if isinstance(biz_class, str):
            return self._bindings[biz_class].is_bound
        else:
            return self._bindings[get_class_name(biz_class)].is_bound
