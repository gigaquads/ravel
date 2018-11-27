import codecs
import pickle

import grpc

from typing import Text

from appyratus.util import TextTransform
from appyratus.schema import fields, Schema


class GrpcClient(object):
    def __init__(self, registry: 'GrpcRegistry'):
        assert registry.is_bootstrapped
        self._registry = registry
        self._channel = grpc.insecure_channel(registry.client_addr)
        self._grpc_stub = registry.pb2_grpc.GrpcRegistryStub(self._channel)
        self._funcs = {
            p.name: self._build_func(p)
            for p in registry.proxies}

    def __getattr__(self, func_name: Text):
        return self._funcs[func_name]

    def _build_func(self, proxy):
        key = TextTransform.camel(proxy.name)
        request_type = getattr(self._registry.pb2, '{}Request'.format(key))
        send_request = getattr(self._grpc_stub, proxy.name)

        def func(**kwargs):
            # prepare and send the request
            req = request_type(**kwargs)
            resp = send_request(req)
            # translate the native proto response message to a plain dict
            result = {}
            for field_name, field in proxy.response_schema.fields.items():
                value = getattr(resp, field_name, None)
                if value is None:
                    result[field_name] = None
                elif isinstance(field, fields.Dict):
                    decoded_value = codecs.decode(value, 'base64')
                    result[field_name] = pickle.loads(decoded_value)
                # the following should be enabled and expanded when this function becomes recursive
                #elif isinstance(field, Schema):
                #elif isinstance(field, fields.Nested)
                #    field.nested.schema
                else:
                    result[field_name] = value
            # return the response dict
            return result

        return func
