from typing import Dict, List, Type

from pybiz.dao import Dao


class DataAccessLayer(object):
    """
    Stores and manages a global registry, entailing which BizObject class is
    associated with which Dao class.
    """

    def __init__(self):
        self._bizobj_type_2_dao_type = {}
        self._bizobj_type_2_dao_kwargs = {}
        self._bizobj_type_2_instance = {}

    def register(
        self,
        bizobj_type: Type['BizObject'],
        dao_type: Type['BizObject'],
        dao_kwargs: Dict = None
    ):
        self._bizobj_type_2_dao_type[bizobj_type] = dao_type
        self._bizobj_type_2_dao_kwargs[bizobj_type] = dao_kwargs or {}

    def get_dao(self, bizobj_type: Type['BizObject']) -> Dao:
        dao = self._bizobj_type_2_instance.get(bizobj_type)
        if dao is not None:
            return dao
        if bizobj_type not in self._bizobj_type_2_dao_type:
            raise KeyError(
                'Unable to find "{}" in dao classes. '
                'Hint: did you create a manifest file?'.format(
                    bizobj_type.__name__
                )
            )
        # lazily instantiate the Dao class or use the instance
        # object provided by __dao__.
        dao_obj = self._bizobj_type_2_dao_type[bizobj_type]
        if isinstance(dao_obj, type):
            kwargs = self._bizobj_type_2_dao_kwargs[bizobj_type]
            dao_type = dao_obj
            dao = dao_type(**kwargs)
        else:
            dao = dao_obj

        self._bizobj_type_2_instance[bizobj_type] = dao
        dao.bind(bizobj_type)
        return dao

    def is_registered(self, bizobj_type: Type['BizObject']) -> bool:
        return bizobj_type in self._bizobj_type_2_dao_type
