import asyncio
import websockets
import ujson

from typing import List, Type, Dict, Tuple, Text

from pybiz.util import JsonEncoder

from ..async_server_registry import AsyncServerRegistry


class WebsocketServerRegistry(AsyncServerRegistry):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def on_bootstrap(self, host: Text = None, port: int = None):
        super().on_bootstrap(server=websockets.serve(self.serve, host, port))

    def on_start(self):
        self.server = websockets.serve(self.serve, self.host, self.port)

    def on_request(self, proxy, socket, request: Dict) -> Tuple[Tuple, Dict]:
        args = tuple()
        kwargs = request.get('params', {}) or {}
        kwargs['socket'] = socket
        return (args, kwargs)

    def on_response(self, proxy, result, *args, **kwargs):
        return ujson.dumps(result).encode('utf-8')

    async def serve(self, socket, path):
        async for message in socket:
            print(f'>>> Processing: {message}')

            # decode raw request bytes
            try:
                request = JsonEncoder.decode(message)
            except ValueError as exc:
                print(f'invalid request data:  {message}')

            # route the request to the appropriate
            # proxy and await response
            proxy = self.proxies.get(request['method'])
            if proxy is None:
                print(f'>>> Unrecognized method: {request["method"]}')
            else:
                result = await proxy(socket, request)
                await socket.send(result)
