import inspect
import re

from google.protobuf.internal.python_message import (
    GeneratedProtocolMessageType
    )


class GrpcDriver(object):
    """
    GrpcDriver encapsulates and manages access to the autogeneated gRPC python
    modules for a given gRPC service. In particular, it:

        1. Aggregates all protobuf message types into a dict that can also be
           accessed by name, like self.types.MyMessage.
        2. Binds the Servicer class to `Servicer`.
        3. Binds the Stub class to `Stub`.
        4. Binds the add_Servicer_to_server method.
    """

    RE_SERVICER_NAME = re.compile(r'^([\w_]+)Servicer$')
    RE_STUB_NAME = re.compile(r'^([\w_]+)Stub$')
    RE_ADD_SERVICER_METHOD = re.compile(r'^add_([\w_]+)Servicer_to_server$')

    def __init__(self, pb2, pb2_grpc):
        self.pb2 = pb2
        self.pb2_grpc = pb2_grpc
        self.types = self._aggregate_message_types()
        self._bind_pb2_grpc_objects(pb2_grpc)

    def _aggregate_message_types(self, pb2):
        """ Extract protobuf message types from pb2 mobule and store them
        on a dict whose keys can be accessed by name, like a named tuple.
        """
        types = NamedDict()

        for attr, obj in inspect.getmembers(pb2):
            if isinstance(obj, GeneratedProtocolMessageType):
                types[attr] = obj

        return types

    def _bind_pb2_grpc_objects(self, pb2_grpc):
        """ Extract service component classes from pb2_grpc module and set them
            as instance attributes.
        """
        self.Servicer = None
        self.Stub = None
        self.add_Servicer_to_server = None

        for attr, obj in inspect.getmembers(pb2_grpc):
            if inspect.isclass(obj):
                match = self.RE_SERVICER_NAME.match(attr)
                if match:
                    name = match.group()
                    self.Servicer = obj
                    continue
                match = self.RE_STUB_NAME.match(attr)
                if match:
                    name = match.group()
                    self.Stub = obj
                    continue
            elif inspect.isfunction(obj):
                match = self.RE_ADD_SERVICER_METHOD.match(attr)
                if match:
                    name = match.group()
                    self.add_Servicer_to_server = obj
                    continue

        assert self.Servicer is not None
        assert self.Stub is not None
        assert self.add_Servicer_to_server is not None


class NamedDict(dict):

    def __getattr__(self, key):
        return self[key]

    def __setattr__(self, key, value):
        self[key] = value
