import asyncio

import uvloop

from typing import Type, Coroutine

from .base import Api, AsyncProxy


asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())


class AsyncServer(Api):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.server = None
        self.loop = None

    @property
    def proxy_type(self) -> Type['ApiProxy']:
        return AsyncProxy

    def on_bootstrap(self, server):
        self.server = server

    def on_start(self):
        print('>>> Entering async server loop...')
        try:
            self.loop = asyncio.get_event_loop()
            self.loop.run_until_complete(self.server)
            self.loop.run_forever()
        finally:
            self.loop.close()
