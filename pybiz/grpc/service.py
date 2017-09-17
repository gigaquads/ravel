import inspect

import grpc

from concurrent.futures import ThreadPoolExecutor, Executor

from .driver import GrpcDriver


class GrpcService(object):
    """
    GrpcService must provide the server implementation of a gRPC service, as
    defined in the service protobuf file. It creates, manages, and exposes a
    gRPC server object. To get or create the server, just use the `server`
    instance property.
    """

    DEFAULT_SERVER_MAX_WORKERS = 8

    def __init__(
            self,
            driver: GrpcDriver,
            insecure_port: str=None,
            secure_port: str=None,
            executor: Executor=None):

        assert insecure_port or secure_port

        self._driver = driver
        self._insecure_port = insecure_port
        self._secure_port = secure_port
        self._server = None
        self._new_executor = lambda: executor or ThreadPoolExecutor(
                max_workers=self.DEFAULT_SERVER_MAX_WORKERS
                )

        # ensure the instance implements the expected service interface
        for method_name in self._driver.methods:
            obj = getattr(self, method_name, None)
            if not (obj and inspect.ismethod(obj)):
                raise NotImplemented('{} must implement {}'.format(
                    self.__class__.__name__, method_name))

    @property
    def types(self):
        return self._driver.types

    @property
    def insecure_port(self):
        return self._insecure_port

    @property
    def secure_port(self):
        return self._secure_port

    @property
    def server(self):
        return self._build_server()

    def _build_server(self):  # TODO: rename this because its too similar to _build_grpc_server
        if self._server is not None:
            return self._server

        # build a new Servier class that inherits from the generated grpc
        # servicer class and mixes in the method implementations from
        # this instance's class:
        grpc_servicer_class = self._driver.Servicer
        this_service_class = self.__class__

        class DynamicServicer(this_service_class, grpc_servicer_class):
            pass

        dynamic_servicer = DynamicServicer(
            driver=self._driver,
            insecure_port=self._insecure_port,
            secure_port=self._secure_port,
            )

        self._server = self._build_grpc_server()
        self._driver.add_Servicer_to_server(dynamic_servicer, self._server)

        return self._server

    def _build_grpc_server(self):
        """
        Build and return a grpc server object using a secure or insecure port,
        as defined by the secure_port or insecure_port passed to the
        constructor.
        """
        server = grpc.server(self._new_executor())

        if self._secure_port is not None:
            server.add_secure_port(self._secure_port)
        elif self._insecure_port is not None:
            server.add_insecure_port(self._insecure_port)

        return server
