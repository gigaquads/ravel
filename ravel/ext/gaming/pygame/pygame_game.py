import os

# hide pygame's stupid "welcome message"
os.environ['PYGAME_HIDE_SUPPORT_PROMPT'] = 'hide'

from functools import reduce
from typing import List
from collections import defaultdict
from logging import getLogger

from appyratus.utils import SysUtils
from appyratus.utils import DictObject
from appyratus.enum import Enum

import pygame as pg

from pygame.time import Clock

from ravel.util.misc_functions import flatten_sequence
from ravel.app.base import Application, ActionDecorator, Action

pygame_logger = getLogger('pygame')

# Generic game components
# TODO: move to more generic location within ravel
EVENT_TYPE = Enum(
    STARTED=1,
    STOPPED=2,
    PAUSED=3,
    RESUMED=4,
    KEY_UP=5,
    KEY_DOWN=5,
    QUIT=6,
)

KEY_EVENT_TYPES = {
    EVENT_TYPE.KEY_UP,
    EVENT_TYPE.KEY_DOWN,
}


class GameEvent(object):
    def __init__(self, event_type, **kwargs):
        self.event_type = event_type
        self.kwargs = kwargs

    def __getattr__(self, name):
        return self.kwargs.get(name)

    def __setattr__(self, name, value):
        self.kwargs[name] = value


# Pygame-specific constants:
# TODO: move into constants.py file
GAME_STARTED = pg.USEREVENT + 0
GAME_PAUSED = pg.USEREVENT + 1
GAME_RESUMED = pg.USEREVENT + 2
GAME_STOPPED = pg.USEREVENT + 3

PYGAME_2_RAVEL_EVENT_TYPE = {
    pg.QUIT: EVENT_TYPE.QUIT,
    pg.KEYUP: EVENT_TYPE.KEY_UP,
    pg.KEYDOWN: EVENT_TYPE.KEY_DOWN,
    GAME_STARTED: EVENT_TYPE.STARTED,
    GAME_PAUSED: EVENT_TYPE.PAUSED,
    GAME_RESUMED: EVENT_TYPE.RESUMED,
    GAME_STOPPED: EVENT_TYPE.STOPPED,
}

PYGAME_WINDOW_EVENT_TYPES = {pg.QUIT}    # TODO
PYGAME_MOUSE_EVENT_TYPES = {}    # TODO
PYGAME_KEY_EVENT_TYPES = {pg.KEYUP, pg.KEYDOWN}


def get_pygame_events():
    events = {}
    for e in dir(pg):
        value = getattr(pg, e)
        if isinstance(value, int):
            events[value] = e
    return events


PYGAME_EVENT_ID_2_NAME = get_pygame_events()


def pygame_2_ravel_event(pygame_event):
    event_type = PYGAME_2_RAVEL_EVENT_TYPE.get(pygame_event.type)
    if event_type is not None:
        ravel_event = GameEvent(event_type)
        #if pygame_event.type in PYGAME_KEY_EVENT_TYPES:
        #    ravel_event.key = pygame_event.key
    else:
        ravel_event = GameEvent(pygame_event.type, **pygame_event.dict)
    return ravel_event


# ------


class PygameGame(Application):
    """
    key_event_handlers, events defined in action `key` attribute
    any_key_evnet_handlers, 
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.key_event_handlers = defaultdict(lambda: defaultdict(list))
        self.any_key_event_handlers = defaultdict(lambda: list)
        self.mouse_event_handlers = defaultdict(lambda: list)
        self.window_event_handlers = defaultdict(
            list, {EVENT_TYPE.QUIT: [lambda event: {
                'is_running': False
            }]}
        )
        self.local.pygame_state = DictObject()

    @property
    def state(self):
        return self.local.pygame_state

    def on_decorate(self, handler: 'Action'):
        event_type = handler.decorator.kwargs['event']
        print('KEY EVENT TYPES', KEY_EVENT_TYPES)
        if event_type in KEY_EVENT_TYPES:
            # look for action `key` attribute and add to "key" event handlers.
            # otherwise add to "any key" event handlers.
            key_obj = handler.decorator.kwargs.get('key')
            if key_obj is not None:
                for key in set(flatten_sequence(key_obj)):
                    self.key_event_handlers[event_type][key].append(handler)
            else:
                self.any_key_event_handlers[event_type].append(handler)
            return

    #
    # def on_request(
    #     self,
    #     action: 'Action',
    #     *raw_args,
    #     **raw_kwargs
    # ) -> Tuple[Tuple, Dict]:

    def on_bootstrap(self, window_size=None, display_mode_flags=None, fps=34):
        self.state.clock = Clock()
        self.state.window_size = tuple(window_size or (800, 600))
        self.state.is_running = False
        self.state.fps = fps
        self.state.delta_t = None
        self.state.display_mode_flags = (
            reduce(lambda x, y: x | y, display_mode_flags)
            if display_mode_flags and len(display_mode_flags) > 1
            else (display_mode_flags or pg.DOUBLEBUF)
        )

        pg.init()

    def on_extract(self, action, index, parameter, raw_args, raw_kwargs):
        # API methods are not called with any arguments; hence, all prepared
        # arguments are plucked from the global game state dict.
        if raw_args and index == 0 and parameter.name == 'event':
            event = raw_args[0]
            return event
        if index is not None:
            return self.state[parameter.name]
        else:
            return self.state.get(parameter.name)

    def on_start(self):
        self.state.tick = 0
        self.state.is_running = True
        self.state.screen = pg.display.set_mode(
            self.state.window_size, self.state.display_mode_flags
        )

        start_event = pg.event.Event(EVENT_TYPE.STARTED)
        pg.event.post(start_event)

        while self.state.is_running:
            self.state.delta_t = self.state.clock.tick(self.state.fps)
            self.state.tick += 1

            for pygame_event in pg.event.get():
                # convert native pygame event to generic Ravel game event
                event = pygame_2_ravel_event(pygame_event)
                if not event:
                    continue

                # route ravel event to appropriate handler/s
                if event.event_type in self.window_event_handlers:
                    handler = self.window_event_handlers[event.event_type]
                    fresh_state = handler(event)
                    if fresh_state:
                        self.state.update(fresh_state)
                elif event.event_type in self.key_event_handlers:
                    key_2_handlers = self.key_event_handlers[event.event_type]
                    handlers = key_2_handlers[event.text]
                    for handler in handlers:
                        fresh_state = handler(event)
                        if fresh_state:
                            self.state.update(fresh_state)
                elif event.event_type == PYGAME_MOUSE_EVENT_TYPES:
                    handlers = self.mouse_event_handlers[event.event_type]
                    for handler in handlers:
                        fresh_state = handler(event)
                        if fresh_state:
                            self.state.update(fresh_state)
                elif event.event_type == PYGAME_WINDOW_EVENT_TYPES:
                    handlers = self.window_event_handlers[event.event_type]
                    for handler in handlers:
                        fresh_state = handler(event)
                        if fresh_state:
                            self.state.update(fresh_state)
                elif event.event_type == pg.QUIT:
                    pg.quit()

            dirty_rects = self.on_update(self.state.tick)
            self.on_draw(self.state.tick)

            # TODO: update dirty_rects generated by on_update instead of flip
            pg.display.flip()

    def on_update(self, tick) -> List[pg.Rect]:
        pass

    def on_draw(self, tick):
        pass
