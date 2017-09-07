import pytest

from mock import MagicMock, patch

from google.protobuf.internal.python_message import (
    GeneratedProtocolMessageType
    )

from pybiz.grpc.driver import GrpcDriver


@pytest.fixture(scope='function')
def mock_pb2():
    pb2 = MagicMock()
    pb2.FooType = MagicMock(spec=GeneratedProtocolMessageType)
    pb2.BarType = 'bar'
    return pb2


@pytest.fixture(scope='function')
def mock_pb2_grpc():
    pb2_grpc = MagicMock()
    pb2_grpc.FooStub = MagicMock(spec=type)
    pb2_grpc.FooServicer = MagicMock(spec=type)
    pb2_grpc.add_FooServicer_to_server = lambda: None
    return pb2_grpc


def test_message_type_aggregation(mock_pb2):
    types = GrpcDriver._aggregate_message_types(None, mock_pb2)
    assert isinstance(types, dict)
    assert 'FooType' in types
    assert 'BarType' not in types


def test_bind_pb2_grpc_objects(mock_pb2_grpc):
    mock_driver = MagicMock()
    mock_driver.Stub = None
    mock_driver.Servicer = None
    mock_driver.add_Servicer_to_server = None
    mock_driver.RE_SERVICER_NAME = GrpcDriver.RE_SERVICER_NAME
    mock_driver.RE_STUB_NAME = GrpcDriver.RE_STUB_NAME
    mock_driver.RE_ADD_SERVICER_METHOD = GrpcDriver.RE_ADD_SERVICER_METHOD

    GrpcDriver._bind_pb2_grpc_objects(mock_driver, mock_pb2_grpc)

    assert mock_driver.Servicer is mock_pb2_grpc.FooServicer
    assert mock_driver.Stub is mock_pb2_grpc.FooStub
    assert mock_driver.add_Servicer_to_server is mock_pb2_grpc.add_FooServicer_to_server
