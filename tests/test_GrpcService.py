import pytest

from mock import MagicMock, patch

from google.protobuf.internal.python_message import (
    GeneratedProtocolMessageType
    )

from pybiz.grpc.service import GrpcService


def test_server_property():
    mock_driver = MagicMock()
    mock_driver.Servicer = type('DummyType', (object, ), {})

    class MockService(object):
        def __init__(self, *args, **kwargs):
            self._build_grpc_server = MagicMock()
            self._build_grpc_server.return_value = 'mock_server'
            self._insecure_port = '[::]:5000'
            self._secure_port = '[::]:5001'
            self._driver = mock_driver
            self._server = None

        def method_1(self):
            pass

    mock_service = MockService()

    assert mock_service._server is None
   
    mock_server = GrpcService._build_server(mock_service)

    # ensure we create a grpc server instance
    assert mock_service._server is not None

    mock_driver.add_Servicer_to_server.assert_called_once()

    # verify that the dynamic servicer class inherits the methods of
    # MockService
    dynamic_service = mock_driver.add_Servicer_to_server.call_args[0][0]

    for attr in ['method_1']:
        assert hasattr(dynamic_service, attr)

    # ensure that the server instance is memoized
    mock_server_2 = GrpcService._build_server(mock_service)
    assert mock_server is mock_server_2


def test_build_grpc_server():
    mock_service = MagicMock()
    mock_service._insecure_port = '[::]:5000'
    mock_service._secure_port = None

    with patch('pybiz.grpc.service.grpc') as mock_grpc:
        mock_server = GrpcService._build_grpc_server(mock_service)
        mock_grpc.server.assert_called_once()

        mock_server.add_insecure_port.assert_called_once_with(mock_service._insecure_port)

    mock_service = MagicMock()
    mock_service._insecure_port = None
    mock_service._secure_port = '[::]:5001'

    with patch('pybiz.grpc.service.grpc') as mock_grpc:
        mock_server = GrpcService._build_grpc_server(mock_service)
        mock_server.add_secure_port.assert_called_once_with(mock_service._secure_port)
