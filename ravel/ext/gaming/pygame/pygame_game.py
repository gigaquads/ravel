import os

# hide pygame's silly "wlcome message"
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

KEY_EVENT_TYPES = {
    pg.KEYUP,
    pg.TEXTINPUT,
    pg.KMOD_ALT,
    pg.USEREVENT + 8,
    pg.ACTIVEEVENT,
}

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
MOUSE_EVENT_TYPES = {pg.MOUSEBUTTONDOWN, pg.MOUSEBUTTONUP}    # TODO
PYGAME_KEY_EVENT_TYPES = {pg.KEYUP, pg.KEYDOWN}

GAME_DEFAULTS = Enum(
    FPS=34,
    WINDOW_SIZE=(1600, 600),
    TITLE="My Ravel Game",
)

# scancodes are not reliable in pygame
# http://www.asciitable.com
# https://en.wikipedia.org/wiki/ASCII#ASCII_printable_characters
# http://www.philipstorr.id.au/pcbook/book3/scancode.htm
# https://en.wikipedia.org/wiki/Keyboard_layout
# https://www.win.tue.nl/~aeb/linux/kbd/scancodes-10.html
# https://en.wikipedia.org/wiki/Scancode

GLYPH2USB_SCANCODE_KEYMAP = {
    'a': 4,
    'b': 5,
    'c': 6,
    'd': 7,
    'e': 8,
    'f': 9,
    'g': 10,
    'h': 11,
    'i': 12,
    'j': 13,
    'k': 14,
    'l': 15,
    'm': 16,
    'n': 17,
    'o': 18,
    'p': 19,
    'q': 20,
    'r': 21,
    's': 22,
    't': 23,
    'u': 24,
    'v': 25,
    'w': 26,
    'x': 27,
    'z': 29,
}

ASCII2GLYPH_KEYMAP = {
    'a': 97,
    'b': 98,
    'c': 99,
    'd': 100,
    'e': 101,
    'f': 102,
    'g': 103,
    'h': 104,
    'i': 105,
    'j': 106,
    'k': 107,
    'l': 108,
    'm': 109,
    'n': 110,
    'o': 111,
    'p': 112,
    'q': 113,
    'r': 114,
    's': 115,
    't': 116,
    'u': 117,
    'v': 118,
    'w': 119,
    'x': 120,
    'y': 121,
    'z': 122,
}

GLYPH2PYGAME_KEYMAP = {
    'a': pg.K_a,
    'b': pg.K_b,
    'c': pg.K_c,
    'd': pg.K_d,
    'e': pg.K_e,
    'f': pg.K_f,
    'g': pg.K_g,
    'h': pg.K_h,
    'i': pg.K_i,
    'j': pg.K_j,
    'k': pg.K_k,
    'l': pg.K_l,
    'm': pg.K_m,
    'n': pg.K_n,
    'o': pg.K_o,
    'p': pg.K_p,
    'q': pg.K_q,
    'r': pg.K_r,
    's': pg.K_s,
    't': pg.K_t,
    'u': pg.K_u,
    'v': pg.K_v,
    'w': pg.K_w,
    'x': pg.K_x,
    'y': pg.K_y,
    'z': pg.K_z,
}


def get_glyph_symbols(glyph):
    symbols = [glyph]
    for keymap in (GLYPH2USB_SCANCODE_KEYMAP, ASCII2GLYPH_KEYMAP, GLYPH2PYGAME_KEYMAP):
        if glyph in keymap:
            symbols.append(keymap[glyph])
    return tuple(symbols)


KEYS = Enum(
    {
        'a': get_glyph_symbols('a'),
        'b': get_glyph_symbols('b'),
        'c': get_glyph_symbols('c'),
        'd': get_glyph_symbols('d'),
        'e': get_glyph_symbols('e'),
        'f': get_glyph_symbols('f'),
        'g': get_glyph_symbols('g'),
        'h': get_glyph_symbols('h'),
        'i': get_glyph_symbols('i'),
        'j': get_glyph_symbols('j'),
        'k': get_glyph_symbols('k'),
        'l': get_glyph_symbols('l'),
        'm': get_glyph_symbols('m'),
        'n': get_glyph_symbols('n'),
        'o': get_glyph_symbols('o'),
        'p': get_glyph_symbols('p'),
        'q': get_glyph_symbols('q'),
        'r': get_glyph_symbols('r'),
        's': get_glyph_symbols('s'),
        't': get_glyph_symbols('t'),
        'u': get_glyph_symbols('u'),
        'v': get_glyph_symbols('v'),
        'w': get_glyph_symbols('w'),
        'x': get_glyph_symbols('x'),
        'y': get_glyph_symbols('y'),
        'z': get_glyph_symbols('z'),
    }
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
        self.__dict__['kwargs'] = kwargs or {}
        self.event_type = event_type
        console.debug(f'Event {event_type} mapping: "{self.get_event()}"', data=kwargs)

    def __getattr__(self, name):
        return self.__dict__['kwargs'].get(name)

    def __setattr__(self, name, value):
        self.kwargs[name] = value

    def get_event(self):
        return PYGAME_EVENT_ID_2_NAME.get(self.event_type)

    @property
    def char(self):
        try:
            text = self.unicode
            #chr(self.key)
        except ValueError as exc:
            text = ''
        return text


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
    any_key_event_handlers, 
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.key_event_handlers = defaultdict(lambda: defaultdict(list))
        self.any_key_event_handlers = defaultdict(lambda: list())
        self.misc_event_handlers = defaultdict(lambda: list())
        self.mouse_event_handlers = defaultdict(lambda: list())
        self.user_event_handlers = defaultdict(lambda: list())
        self.window_event_handlers = defaultdict(list, {EVENT_TYPE.QUIT: [lambda event: {'is_running': False}]})
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

        if event_type in MOUSE_EVENT_TYPES:
            self.mouse_event_handlers[event_type].append(handler)

        if event_type >= pg.USEREVENT:
            self.user_event_handlers[event_type].append(handler)

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
            reduce(lambda x, y: x | y, display_mode_flags) if display_mode_flags and len(display_mode_flags) > 1 else
            (display_mode_flags or pg.DOUBLEBUF)
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
        self.state.screen = pg.display.set_mode(self.state.window_size, self.state.display_mode_flags)
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
                event_type = event.event_type

                # route ravel event to appropriate handler/s
                if event_type in self.window_event_handlers:
                    handlers = self.window_event_handlers[event_type]
                    for handler in handlers:
                        fresh_state = handler(event)
                        if fresh_state:
                            self.state.update(fresh_state)

                # events that have a key attached to them
                if event_type in self.key_event_handlers and event.key:
                    key_2_handlers = self.key_event_handlers[event_type]
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

                if event_type in self.any_key_event_handlers and event.key:
                    handlers = self.any_key_event_handlers[event_type]
                    print('found handlers', handlers)
                    print('event', event_type, event.key, event.text, event.unicode)
                    for handler in handlers:
                        fresh_state = handler(event)
                        if fresh_state:
                            self.state.update(fresh_state)

                if event_type in self.user_event_handlers:
                    handlers = self.user_event_handlers[event_type]
                    for handler in handlers:
                        fresh_state = handler(event)
                        if fresh_state:
                            self.state.update(fresh_state)

                if event_type in MOUSE_EVENT_TYPES:
                    handlers = self.mouse_event_handlers[event_type]
                    for handler in handlers:
                        fresh_state = handler(event)
                        if fresh_state:
                            self.state.update(fresh_state)

                if event_type in PYGAME_WINDOW_EVENT_TYPES:
                    handlers = self.window_event_handlers[event_type]
                    for handler in handlers:
                        fresh_state = handler(event)
                        if fresh_state:
                            self.state.update(fresh_state)

                if event_type == pg.QUIT:
                    pg.quit()

            dirty_rects = self.on_update(self.state.tick)
            self.on_draw(self.state.tick)

            # TODO: collect dirty_rects
            # TODO: update dirty_rects generated by on_update instead of flip
            pg.display.flip()

    def on_update(self, tick) -> List[pg.Rect]:
        pass

    def on_draw(self, tick):
        console.debug(f'Tick {self.state.tick}')
