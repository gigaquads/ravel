import asyncio
import websockets
import ujson

from typing import List, Type, Dict, Tuple, Text

from .async_server_registry import AsyncServerRegistry


class WebSocketServerRegistry(AsyncServerRegistry):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._name2praxy = {}

    def on_decorate(self, proxy: 'RegistryProxy'):
        self._name2praxy[proxy.name] = proxy

    def on_request(self, proxy, socket, request: Dict) -> Tuple[Tuple, Dict]:
        args = tuple()
        kwargs = request.get('params', {}) or {}
        kwargs['socket'] = socket
        return (args, kwargs)

    def on_response(self, proxy, result, *args, **kwargs):
        return ujson.dumps(result).encode('utf-8')

    def start(self, host: Text, port: int):
        server = websockets.serve(self.serve, host, port)
        super().start(server)

    async def serve(self, socket, path):
        async for message in socket:
            print(f'>>> Processing: {message}')

            # decode raw request bytes
            try:
                request = ujson.loads(message)
            except ValueError as exc:
                print(f'invalid request data:  {message}')

            # route the request to the appropriate
            # proxy and await response
            proxy = self._name2praxy.get(request['method'])
            if proxy is None:
                print(f'>>> Unrecognized method: {request["method"]}')
            else:
                result = await proxy(socket, request)
                await socket.send(result)
