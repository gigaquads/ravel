from typing import Text, Type, List, Set, Dict, Tuple
from copy import deepcopy
from datetime import datetime

from .base import Dao

# TODO: Move rev into record instead of having separate cache_fetch method
# TODO: Modify relationship query method to use fetch_all or fetch if a simply
# select by ID predicate is used, thereby utilizing cache


class CacheInterface(object):
    def cache_fetch(self, _ids: Set, fetch=False, fields=None) -> Dict:
        raise NotImplementedError()


class CacheRecord(object):
    def __init__(self, rev: int, record: Dict = None):
        self.rev = rev
        self.record = record


class CacheDao(Dao):
    def __init__(self, local: Dao, remote: Dao, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._local_dao = local
        self._remote_dao = remote

    def bind(self, bizobj_type):
        super().bind(bizobj_type)
        self._local_dao.bind(bizobj_type)
        self._remote_dao.bind(bizobj_type)

    def fetch(self, _id, fields=None):
        local = self._local_dao.cache_fetch(
            [_id], fetch=True, fields=fields
        ).get(_id)

        if local:
            remote = self._remote_dao.cache_fetch([_id]).get(_id)
            if remote and remote.rev > local.rev:
                record = self._remote_dao.fetch(_id, fields=fields)
                self._local_dao.update(_id, record)
            elif fields:
                record = {k: local.record[k] for k in fields}
            else:
                record = local.record
        else:
            record = self._remote_dao.fetch(_id, fields=None)
            self._local_dao.create(record)

        return deepcopy(record)

    def fetch_many(self, _ids, fields=None):
        local = self._local_dao.cache_fetch(_ids, fetch=True, fields=fields)
        remote = self._remote_dao.cache_fetch(local.keys())
        records = {}
        for _id, cache_record in local.items():
            if cache_record.rev < remote[_id].rev:
                stale_ids.add(_id)
            else:
                records[_id] = kk


    def query(self, predicate, **kwargs):
        """
        """

    def exists(self, _id) -> bool:
        """
        Return True if the record with the given _id exists.
        """

    def fetch_many(self, _ids: List, fields: Dict = None) -> Dict:
        """
        Read multiple records by _id, selecting only the designated fields (or
        all by default).
        """

    def create(self, data: Dict) -> Dict:
        """
        Create a new record with the _id. If the _id is contained is not
        contained in the data dict nor provided as the _id argument, it is the
        responsibility of the Dao class to generate the _id.
        """

    def create_many(self, records: List[Dict]) -> None:
        """
        Create a new record.  It is the responsibility of the Dao class to
        generate the _id.
        """

    def update(self, _id, data: Dict) -> Dict:
        record = self._local_dao.update(_id, data)
        self._remote_dao.update(_id, data)
        return record

    def update_many(self, _ids: List, data: List[Dict] = None) -> None:
        """
        Update multiple records. If a single data dict is passed in, then try to
        apply the same update to all records; otherwise, if a list of data dicts
        is passed in, try to zip the _ids with the data dicts and apply each
        unique update or each group of identical updates individually.
        """

    def delete(self, _id) -> None:
        """
        Delete a single record.
        """

    def delete_many(self, _ids: List) -> None:
        """
        Delete multiple records.
        """





if __name__ == '__main__':
    from pybiz.biz import BizObject
    from pybiz.schema import Int, String
    from pybiz.api.repl import ReplRegistry
    from pybiz.dao.dict_dao import DictDao

    class User(BizObject):
        name = String()
        age = Int()

    dao = CacheDao(local=DictDao(), remote=DictDao())
    dao.update(1, {'_id': 1, 'name': 'a', 'age': 2})

    repl = ReplRegistry()
    repl.manifest.process()
    repl.start({
        'User': User,
        'dao': dao,
    })
