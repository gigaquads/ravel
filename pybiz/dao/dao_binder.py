from typing import Dict, List, Type, Set, Text

from pybiz.dao import Dao
from pybiz.util import is_sequence


class DaoBinding(object):
    def __init__(self, biz_type, dao_instance, dao_bind_kwargs=None):
        self.biz_type = biz_type
        self.dao_instance = dao_instance
        self.dao_bind_kwargs = dao_bind_kwargs if dao_bind_kwargs is not None else {}
        self._is_bound = False

    def __repr__(self):
        return f'<DaoBinding({self.biz_type_name}, {self.dao_type_name})>'

    def bind(self):
        self.dao_instance.bind(self.biz_type, **self.dao_bind_kwargs)
        self._is_bound = True

    @property
    def is_bound(self):
        return self._is_bound

    @property
    def dao_type(self):
        return self.dao_instance.__class__

    @property
    def dao_type_name(self):
        return self.dao_type.__name__

    @property
    def biz_type_name(self):
        return self.biz_type.__name__


class DaoBinder(object):
    """
    Stores and manages a global registry, entailing which BizObject class is
    associated with which Dao class.
    """

    def __init__(self):
        self._bindings = {}

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
    def bindings(self):
        return list(self._bindings.values())

    def register(
        self,
        biz_type: Type['BizObject'],
        dao_instance: Dao,
        dao_bind_kwargs: Dict = None,
    ):
        self._bindings[biz_type.__name__] = DaoBinding(
            biz_type=biz_type,
            dao_instance=dao_instance,
            dao_bind_kwargs=dao_bind_kwargs,
        )

    def bind(self, biz_types: Set[Type['BizObject']] = None):
        if not biz_types:
            biz_types = [v.biz_type for v in self._bindings.values()]
        elif not is_sequence(biz_types):
            biz_types = [biz_types]
        for biz_type in biz_types:
            self.get_dao_instance(biz_type)

    def get_dao_type_by_name(dao_type_name: Text) -> Type[Dao]:
        return self._dao_types[dao_type_name]

    def get_dao_instance(self, biz_type: Type['BizObject'], bind=True) -> Dao:
        binding = self._bindings.get(biz_type.__name__)
        if binding is None:
            raise Exception(f'{biz_type} has no registered Dao binding')
        if bind and (not binding.is_bound):
            binding.bind()
        return binding.dao_instance

    def is_registered(self, biz_type: Type['BizObject']) -> bool:
        return biz_type.__name__ in self._bindings

    def is_bound(self, biz_type: Type['BizObject']) -> bool:
        return self._bindings[biz_type.__name__].is_bound
