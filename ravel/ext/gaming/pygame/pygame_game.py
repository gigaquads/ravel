import os

# hide pygame's stupid "welcome message"
os.environ['PYGAME_HIDE_SUPPORT_PROMPT'] = 'hide'

from functools import reduce
from typing import List
from collections import defaultdict
from logging import getLogger

from appyratus.utils.sys_utils import SysUtils
from appyratus.utils.dict_utils import DictObject
from appyratus.utils.time_utils import TimeUtils

from appyratus.enum import Enum

import pygame as pg

from pygame.time import Clock

from ravel.util.misc_functions import flatten_sequence
from ravel.util.loggers import console
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

KEY_EVENT_TYPES = {pg.KEYUP, pg.TEXTINPUT, pg.KMOD_ALT}

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

GAME_DEFAULTS = Enum(
    FPS=34,
    WINDOW_SIZE=(800, 600),
    TITLE="My Game",
)


class GameEvent(object):
    """
    # Init process
    Event 4352 AUDIODEVICEADDED {}      
    Event 4352 AUDIODEVICEADDED {}      
    Event 32770 VIDEOEXPOSE {}          
    Event 32768 ACTIVEEVENT {'gain': 1, 'state': 1, 'window': None}
    Event 32770 VIDEOEXPOSE {}          
    Event 32770 VIDEOEXPOSE {}          
    Event 1 SCRAP_SELECTION {}          
    Event 32770 VIDEOEXPOSE {}

    # Arrow keys
    Event 768 KMOD_ALT { "key": 1073741903, "mod": 0, "scancode": 79, "unicode": "", "window": null } 
    Event 769 KEYUP { "key": 1073741903, "mod": 0, "scancode": 79, "window": null }

    # Alphanumeric keys
    Event 768 KMOD_ALT { "key": 97, "mod": 0, "scancode": 4, "unicode": "a", "window": null }           
    Event 771 TEXTINPUT { "text": "a", "window": null }
    Event 769 KEYUP { "key": 97, "mod": 0, "scancode": 4, "window": null }

    # Moving the mouse
    Event 1024 MOUSEMOTION {'pos': (699, 0), 'rel': (10, -6), 'buttons': (0, 0, 0), 'window': None}

	# Upon exit
    Event 512 WINDOWEVENT { "event": 14, "window": null }
    Event 256 QUIT

    """

    def __init__(self, event_type, **kwargs):
        console.debug(
            f'Event {event_type} {PYGAME_EVENT_ID_2_NAME[event_type]}', data=kwargs
        )
        self.__dict__['kwargs'] = kwargs or {}
        self.event_type = event_type

    def __getattr__(self, name):
        return self.__dict__['kwargs'].get(name)

    def __setattr__(self, name, value):
        self.kwargs[name] = value


def get_pygame_events():
    events = {}
    for e in dir(pg):
        value = getattr(pg, e)
        if isinstance(value, int):
            events[value] = e
    return events


PYGAME_EVENT_ID_2_NAME = get_pygame_events()


def pygame_2_ravel_event(pygame_event):
    return GameEvent(pygame_event.type, **pygame_event.dict)


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

    def on_bootstrap(
        self,
        window_size=None,
        display_mode_flags=None,
        fps=None,
        title=None,
    ):
        self.state.clock = Clock()
        self.state.is_running = False
        self.state.window_size = tuple(window_size or GAME_DEFAULTS.WINDOW_SIZE)
        self.state.fps = fps or GAME_DEFAULTS.FPS
        self.state.title = title or GAME_DEFAULTS.TITLE
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
        pg.display.set_caption(self.state.title)

        start_event = pg.event.Event(EVENT_TYPE.STARTED)
        pg.event.post(start_event)

        while self.state.is_running:
            self.state.delta_t = self.state.clock.tick(self.state.fps)
            self.state.tick += 1

            # tuple of all pressed keys by key index
            pressed = pg.key.get_pressed()

            # iterate over each pygame event
            pgevents = pg.event.get()
            for pygame_event in pgevents:

                # convert native pygame event to generic Ravel game event
                event = pygame_2_ravel_event(pygame_event)
                if not event:
                    continue

                # route ravel event to appropriate handler/s
                if event.event_type in self.window_event_handlers:
                    handlers = self.window_event_handlers[event.event_type]
                    for handler in handlers:
                        fresh_state = handler(event)
                        if fresh_state:
                            self.state.update(fresh_state)
                elif event.event_type in self.key_event_handlers:
                    key_2_handlers = self.key_event_handlers[event.event_type]
                    # not every event from pygame has a consistent "key" value,
                    # so we will cascade through the known locations as they
                    # are relatively exclusive to certain events. 
                    # TODO this needs to be handled with better mapping of
                    # pygame events to ravel events
                    handlers = key_2_handlers[event.unicode or event.text or event.key]
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
        console.debug(f'Tick {self.state.tick}')
