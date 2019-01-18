import asyncio

import uvloop

from typing import Type, Coroutine

from ..registry import Registry, AsyncRegistryProxy


class AsyncServerRegistry(Registry):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
        self.loop = None

    def start(self, server: Coroutine):
        print('>>> Entering async server loop...')
        try:
            self.loop = asyncio.get_event_loop()
            self.loop.run_until_complete(server)
            self.loop.run_forever()
        finally:
            self.loop.close()

    @property
    def proxy_type(self) -> Type['RegistryProxy']:
        return AsyncRegistryProxy
