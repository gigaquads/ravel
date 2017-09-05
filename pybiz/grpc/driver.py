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
        2. Binds the Servicer class to `servicer_class`.
        3. Binds the Stub class to `stub_class`.
        4. Binds the add_servicer_to_server method.
    """

    RE_SERVICER_NAME = re.compile(r'^([\w_]+)Servicer$')
    RE_STUB_NAME = re.compile(r'^([\w_]+)Stub$')
    RE_ADD_SERVICER_METHOD = re.compile(r'^add_([\w_]+)Servicer_to_server$')

    def __init__(self, pb2, pb2_grpc):
        self.pb2 = pb2
        self.pb2_grpc = pb2_grpc
        self.types = NamedDict()

        # extract protobuf message types from pb2 mobule and store them
        # on a dict whose keys can be accessed by name, like a named tuple.
        for attr, obj in inspect.getmembers(self.pb2):
            if isinstance(obj, GeneratedProtocolMessageType):
                self.types[attr] = obj

        # extract service component classes from pb2_grpc module:
        self.add_servicer_to_server = None
        self.servicer_class = None
        self.stub_class = None

        for attr, obj in inspect.getmembers(self.pb2_grpc):
            match = self.RE_SERVICER_NAME.match(attr)
            if match:
                name = match.group()
                self.servicer_class = obj
                continue
            match = self.RE_STUB_NAME.match(attr)
            if match:
                name = match.group()
                self.stub_class = obj
                continue
            match = self.RE_ADD_SERVICER_METHOD.match(attr)
            if match:
                name = match.group()
                self.add_servicer_to_server = obj
                continue

        assert self.servicer_class is not None
        assert self.stub_class is not None
        assert self.add_servicer_to_server is not None


class NamedDict(dict):

    def __getattr__(self, key):
        return self[key]

    def __setattr__(self, key, value):
        self[key] = value
