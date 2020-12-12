import codecs
import pickle

import grpc

from typing import Text

from appyratus.utils.string_utils import StringUtils

from ravel.util.loggers import console
from ravel.schema import fields, Schema, Field


class GrpcClient(object):
    def __init__(self, app: 'GrpcApplication'):
        assert app.is_bootstrapped

        self._address = app.grpc.options.client_address
        self._app = app

        if app.grpc.options.secure_channel:
            self._channel = grpc.secure_channel(
                self._address, grpc.ssl_channel_credentials()
            )
        else:
            self._channel = grpc.insecure_channel(self._address)

        console.info(
            message='gRPC client initialized',
            data={
                'address': self._address,
                'secure': app.grpc.options.secure_channel
            }
        )

        GrpcApplicationStub = app.grpc.pb2_grpc.GrpcApplicationStub

        self._grpc_stub = GrpcApplicationStub(self._channel)
        self._funcs = {
            k: self._build_func(p)
            for k, p in app.actions.items()
        }

    def __getattr__(self, func_name: Text):
        return self._funcs[func_name]

    def _build_func(self, action):
        key = StringUtils.camel(action.name)
        request_type = getattr(self._app.grpc.pb2, f'{key}Request')
        send_request = getattr(self._grpc_stub, action.name)

        def func(**kwargs):
            # prepare and send the request
            request = request_type(**kwargs)
            response = send_request(request)
            # translate the native proto response message to a plain dict
            if action.streams_response:
                data = [
                    self._extract_fields(x, action.schemas.response)
                    for x in response
                ]
            else:
                data = self._extract_fields(response, action.schemas.response)
            return data

        return func

    def _extract_fields(self, message, schema):
        result = {}
        for field_name, field in schema.fields.items():
            value = getattr(message, field_name, None)
            if isinstance(field, fields.Dict):
                decoded_value = codecs.decode(value, 'base64')
                result[field_name] = pickle.loads(decoded_value)
            elif isinstance(field, fields.Nested):
                result[field_name] = self._extract_fields(value, field.schema)
            elif isinstance(field, fields.List):
                if isinstance(field.nested, Schema):
                    result[field_name] = [
                        self._extract_fields(v, field.nested) for v in value
                    ]
                elif isinstance(field.nested, fields.Dict):
                    result[field_name] = [
                        pickle.loads(codecs.decode(v, 'base64')) for v in value
                    ]
                else:
                    result[field_name] = list(value)
            elif isinstance(field, fields.Set):
                if isinstance(field.nested, Schema):
                    result[field_name] = {
                        self._extract_fields(v, field.nested)
                        for v in value
                    }
                elif isinstance(field.nested, fields.Dict):
                    result[field_name] = {
                        pickle.loads(codecs.decode(v, 'base64'))
                        for v in value
                    }
                else:
                    result[field_name] = set(value)
            elif isinstance(field, Schema):
                result[field_name] = self._extract_fields(value, field)
            else:
                result[field_name] = value
        return result
