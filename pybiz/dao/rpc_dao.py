import ujson

from typing import Dict, List, Text

from appyratus.json import JsonEncoder

from pybiz.predicate import Predicate

from .base import Dao


class RpcDao(Dao):

    @staticmethod
    def __rpc__() -> 'RpcRegistry':
        raise NotImplementedError(
            'return an RpcRegistry instance for the remote service'
        )

    @classmethod
    def __manifest__() -> Text:
        raise NotImplementedError(
            'return the path to the remove manifest.yml file'
        )

    @classmethod
    def __dao__() -> Text:
        raise NotImplementedError(
            'return the name of the remote Dao class'
        )

    def __init__(self):
        super().__init__()
        self._remote_manifest_filepath = self.__manifest__()
        self._remote_dao_type = self.__dao__()
        self._remote_rpc = self.__rpc__()
        self._remote_rpc.bootstrap(self._remote_manifest_filepath)
        self._rpc_client = self._remote_rpc.client
        self._json_encoder = JsonEncoder()

    def _send_rpc_request(self, method_name: Text, args: Dict):
        args_json = self._json_encoder.encode(args)
        result = self._rpc_client.apply_dao_method(
            dao_type=self._remote_dao_type,
            method_name=method_name,
            args_json=args_json,
        )
        return ujson.loads(result['data'])

    def exists(self, _id) -> bool:
        args = {'_id': _id}
        return self._send_rpc_request('exists', args)

    def fetch(self, _id, fields=None) -> Dict:
        args = {'_id': _id, 'fields': fields}
        return self._send_rpc_request('fetch', args)

    def fetch_many(self, _ids, fields=None) -> List:
        args = {'_ids': _ids, 'fields': fields}
        return self._send_rpc_request('fetch_many', args)

    def create(self, record: Dict) -> Dict:
        args = {'record': record}
        return self._send_rpc_request('create', args)

    def create_many(self, records: List[Dict]) -> Dict:
        args = {'records': records}
        return self._send_rpc_request('create_many', args)

    def update(self, _id=None, data: Dict = None) -> Dict:
        args = {'_id': _id, 'data': data}
        return self._send_rpc_request('update', args)

    def update_many(self, _ids: List, data: List = None) -> List:
        args = {'_ids': _id, 'data': data}
        return self._send_rpc_request('update_many', args)

    def delete(self, _id) -> Dict:
        args = {'_id': _id}
        return self._send_rpc_request('delete', args)

    def delete_many(self, _ids: List) -> List:
        args = {'_ids': _ids}
        return self._send_rpc_request('delete_many', args)

    def query(self, predicate, **kwargs):
        args = {'predicate': Predicate.serialize(predicate)}
        return self._send_rpc_request('query', args)
