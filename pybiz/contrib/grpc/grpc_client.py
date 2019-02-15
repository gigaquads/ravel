import codecs
import pickle

import grpc

from typing import Text

from appyratus.utils import StringUtils
from appyratus.schema import fields


class GrpcClient(object):
    def __init__(self, registry: 'GrpcRegistry'):
        assert registry.is_bootstrapped
        self._address = registry.grpc.options.client_address
        self._registry = registry

        print('Connecting to {}'.format(self._address))
        if registry.grpc.options.secure_channel:
            self._channel = grpc.secure_channel(
                self._address, grpc.ssl_channel_credentials()
            )
        else:
            self._channel = grpc.insecure_channel(self._address)

        self._grpc_stub = registry.pb2_grpc.GrpcRegistryStub(self._channel)
        self._funcs = {
            k: self._build_func(p) for k, p in registry.proxies.items()
        }

    def __getattr__(self, func_name: Text):
        return self._funcs[func_name]

    def _build_func(self, proxy):
        key = StringUtils.camel(proxy.name)
        request_type = getattr(self._registry.grpc.pb2, f'{key}Request')
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
