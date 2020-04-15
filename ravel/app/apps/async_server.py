import asyncio

import uvloop

from typing import Type, Coroutine

from ravel.app.base import Application, AsyncAction


asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())


class AsyncServer(Application):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.server = None
        self.loop = None

    @property
    def action_type(self) -> Type['Action']:
        return AsyncAction

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
