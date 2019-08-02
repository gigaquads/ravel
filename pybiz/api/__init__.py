from .registry import Registry, RegistryProxy, RegistryDecorator
from .async_server_registry import AsyncServerRegistry, AsyncRegistryProxy
from .repl_registry import ReplRegistry, ReplFunction
from .cli_registry import CliRegistry, CliCommand
from .web import (
    HttpRegistry, HttpServerRegistry,
    WsgiServiceRegistry, WebsocketServerRegistry
)

Repl = ReplRegistry
Cli = CliRegistry
AsyncServer = AsyncServerRegistry
Http = HttpRegistry
HttpServer = HttpServerRegistry
WsgiService = WsgiServiceRegistry
WebsocketServer = WebsocketServerRegistry
