import asyncio
import websockets

from typing import List, Type, Dict, Tuple, Text

from ravel.util.json_encoder import JsonEncoder

from ..async_server import AsyncServer

json = JsonEncoder()


class WebsocketServer(AsyncServer):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._host = None
        self._port = None

    def on_bootstrap(self, host: Text, port: int):
        self._host = host
        self._port = port
        super().on_bootstrap(
            server=websockets.serve(self.serve, self.host, self.port)
        )

    def on_request(self, action, socket, request: Dict) -> Tuple[Tuple, Dict]:
        args = tuple()
        kwargs = request.get('params', {}) or {}
        kwargs['socket'] = socket
        return (args, kwargs)

    def on_response(self, action, result, *args, **kwargs):
        return json.encode(result).encode('utf-8')

    async def serve(self, socket, path):
        async for message in socket:
            print(f'>>> Processing: {message}')

            # decode raw request bytes
            try:
                request = json.decode(message)
            except ValueError:
                print(f'invalid request data: {message}')

            # route the request to the appropriate
            # action and await response
            action = self.actions.get(request['method'])
            if action is None:
                print(f'>>> Unrecognized method: {request["method"]}')
            else:
                result = await action(socket, request)
                await socket.send(result)
