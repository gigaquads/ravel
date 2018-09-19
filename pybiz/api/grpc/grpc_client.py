import grpc

from typing import Text

from appyratus.util import TextTransform


class GrpcClient(object):
    def __init__(self, registry: 'GrpcFunctionRegistry'):
        assert registry.is_bootstrapped
        self._registry = registry
        self._channel = grpc.insecure_channel(registry.client_addr)
        self._grpc_stub = registry.pb2_grpc.GrpcRegistryStub(self._channel)
        self._funcs = {p.name: self._build_func(p) for p in registry.proxies}

    def __getattr__(self, func_name: Text):
        return self._funcs[func_name]

    def _build_func(self, proxy):
        key = TextTransform.camel(proxy.name)
        request_type = getattr(self._registry.pb2, '{}Request'.format(key))
        send_request = getattr(self._grpc_stub, proxy.name)

        def func(*args, **kwargs):
            # prepare and send the request
            req = request_type(**kwargs)
            resp = send_request(req)
            # translate the response protobuf into a python dict
            return {
                k: getattr(resp, k, None)
                for k in proxy.response_schema.fields
            }

        return func
