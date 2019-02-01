import codecs
import pickle

import grpc

from typing import Text

from appyratus.utils import StringUtils
from appyratus.schema import fields


class GrpcClient(object):
    def __init__(self, registry: 'GrpcRegistry'):
        assert registry.is_bootstrapped
        self._registry = registry

        print('Connecting to {}'.format(registry.client_addr))
        if registry.secure_channel:
            self._channel = grpc.secure_channel(
                registry.client_addr, grpc.ssl_channel_credentials()
            )
        else:
            self._channel = grpc.insecure_channel(registry.client_addr)

        self._grpc_stub = registry.pb2_grpc.GrpcRegistryStub(self._channel)
        self._funcs = {
            k: self._build_func(p)
            for k, p in registry.proxies.items()
        }

    def __getattr__(self, func_name: Text):
        return self._funcs[func_name]

    def _build_func(self, proxy):
        key = StringUtils.camel(proxy.name)
        request_type = getattr(self._registry.pb2, '{}Request'.format(key))
        send_request = getattr(self._grpc_stub, proxy.name)

        def extract_schema_data(data, data_fields):
            result = {}
            for field_name, field in data_fields.items():
                value = getattr(data, field_name, None)
                if value is None:
                    result[field_name] = None
                elif isinstance(field, fields.Dict):
                    decoded_value = codecs.decode(value, 'base64')
                    result[field_name] = pickle.loads(decoded_value)
                elif isinstance(field, fields.List):
                    nested_fields = field.nested.fields
                    result[field_name] = [extract_schema_data(v, nested_fields) for v in value]
                else:
                    result[field_name] = value
            # return the response dict
            return result

        def func(**kwargs):
            # prepare and send the request
            req = request_type(**kwargs)
            resp = send_request(req)
            # translate the native proto response message to a plain dict
            result = extract_schema_data(resp, proxy.response_schema.fields)
            return result

        return func
