from typing import Dict, List, Type, Set, Text

from pybiz.dao import Dao
from pybiz.util.misc_functions import is_sequence
from pybiz.util.loggers import console


class DaoBinding(object):
    def __init__(self, biz_class, dao_instance, dao_bind_kwargs=None):
        self.biz_class = biz_class
        self.dao_instance = dao_instance
        self.dao_bind_kwargs = dao_bind_kwargs or {}
        self._is_bound = False

    def __repr__(self):
        return (
            f'<DaoBinding({self.biz_class_name}, {self.dao_class_name}, '
            f'bound={self.is_bound})>'
        )

    def bind(self, binder):
        self.dao_instance.bind(self.biz_class, **self.dao_bind_kwargs)
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
        return self.dao_class.__name__

    @property
    def biz_class_name(self):
        return self.biz_class.__name__


class DaoBinder(object):
    """
    Stores and manages a global app, entailing which BizObject class is
    associated with which Dao class.
    """

    def __init__(self):
        self._bindings = {}
        self._named_dao_classs = {}
        self._named_biz_classs = {}

    def __repr__(self):
        return f'<DaoBinder(bindings={len(self.bindings)})>'

    @classmethod
    def get_instance(cls):
        """
        Get the global singleton instance.
        """
        attr_name = '_instance'
        singleton = getattr(cls, attr_name, None)
        if singleton is None:
            singleton = cls()
            setattr(cls, attr_name, singleton)
        return singleton

    @property
    def bindings(self) -> List['DaoBinding']:
        return list(self._bindings.values())

    @property
    def biz_classs(self) -> Dict[Text, 'BizObject']:
        return self._named_biz_classs

    @property
    def dao_classs(self) -> Dict[Text, 'Dao']:
        return self._named_dao_classs

    def register(
        self,
        biz_class: Type['BizObject'],
        dao_class: Type[Dao],
        dao_bind_kwargs: Dict = None,
    ):
        dao_class_name = dao_class.__name__
        if dao_class_name not in self._named_dao_classs:
            dao_class = type(dao_class_name, (dao_class, ), {})
            self._named_dao_classs[dao_class_name] = dao_class
            console.debug(
                f'registered Dao "{dao_class_name}" with DaoBinder'
            )

        dao_instance = dao_class()

        if biz_class is not None:
            biz_class_name = biz_class.__name__
            biz_class.binder = self
            self._named_biz_classs[biz_class_name] = biz_class
            self._bindings[biz_class_name] = binding = DaoBinding(
                biz_class=biz_class,
                dao_instance=dao_instance,
                dao_bind_kwargs=dao_bind_kwargs,
            )
            console.debug(
                f'registered BizObject "{biz_class_name}" with DaoBinder'
            )
            return binding

        return None

    def bind(self, biz_classs: Set[Type['BizObject']] = None, rebind=False):
        if not biz_classs:
            biz_classs = [v.biz_class for v in self._bindings.values()]
        elif not is_sequence(biz_classs):
            biz_classs = [biz_classs]
        for biz_class in biz_classs:
            if not biz_class.is_abstract:
                biz_class.binder = self  # this is used in BizObject.get_dao()
                self.get_dao_instance(biz_class, rebind=rebind)

    def get_dao_instance(
        self, biz_class: Type['BizObject'], bind=True, rebind=False
    ) -> Dao:
        if isinstance(biz_class, str):
            binding = self._bindings.get(biz_class)
        else:
            binding = self._bindings.get(biz_class.__name__)

        if binding is None:
            # lazily register a new binding
            base_dao_class = biz_class.__dao__()
            console.debug(
                f'calling {biz_class.__name__}.__dao__()'
            )
            binding = self.register(biz_class, base_dao_class)

        # call bind only if it already hasn't been called
        if rebind or ((not binding.is_bound) and bind):
            console.debug(
                message=(
                    f'binding {binding.dao_instance.__class__.__name__} '
                    f'singleton to {binding.biz_class.__name__} class...'
                )
            )
            binding.bind(binder=self)

        return binding.dao_instance

    def get_dao_class(self, dao_class_name: Text) -> Type[Dao]:
        return self._named_dao_classs.get(dao_class_name)

    def is_registered(self, biz_class: Type['BizObject']) -> bool:
        if isinstance(biz_class, str):
            return biz_class in self._bindings
        else:
            return biz_class.__name__ in self._bindings

    def is_bound(self, biz_class: Type['BizObject']) -> bool:
        if isinstance(biz_class, str):
            return self._bindings[biz_class].is_bound
        else:
            return self._bindings[biz_class.__name__].is_bound
