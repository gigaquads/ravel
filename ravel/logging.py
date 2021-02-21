from __future__ import absolute_import

import json
import logging
import sys
import os

import yaml

from logging import Formatter, StreamHandler, INFO
from threading import current_thread
from typing import Text, Dict

from appyratus.utils.time_utils import TimeUtils
from appyratus.utils.string_utils import StringUtils

from ravel.util.json_encoder import JsonEncoder


class LoggerInterface(object):
    def __init__(self, name, level=None, handlers=None):
        self._name = name
        self._level = level or INFO
        self._formatter = Formatter('%(message)s')

        self._logger = logging.getLogger(self._name)

        if self._level:
            self._logger.setLevel(self._level)
        else:
            self._level = self._logger.level

        if handlers:
            self._handlers = handlers
        else:
            stderr_handler = StreamHandler()
            stderr_handler.setLevel(self._level)
            self._handlers = [stderr_handler]

        for handler in self._handlers:
            if not handler.formatter:
                handler.setFormatter(self._formatter)
            self._logger.addHandler(handler)

    @property
    def logger(self):
        return self._logger

    def process_message(self, level: Text, message: Text, data: Dict) -> Text:
        return message

    def debug(self, message=None, data: Dict = None):
        self._logger.debug(self.process_message('debug', message, data))

    def info(self, message=None, data: Dict = None):
        self._logger.info(self.process_message('info', message, data))

    def warning(self, message=None, data: Dict = None):
        self._logger.warning(self.process_message('warning', message, data))

    def critical(self, message=None, data: Dict = None):
        self._logger.critical(self.process_message('critical', message, data))

    def error(self, message=None, data: Dict = None):
        self._logger.error(self.process_message('error', message, data))

    def exception(self, message=None, data: Dict = None):
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        data = (data if data is not None else{}).update({
            'exc_type': exc_type,
            'fname': fname,
            'lineno': exc_tb.tb_lineno
        })
        self._logger.exception(self.process_message('error', message, data))


class ConsoleLoggerInterface(LoggerInterface):
    def __init__(self, name, level=None, style=None):
        super().__init__(name, level=level)
        self._json = JsonEncoder()
        self._style = style or 'json'

    def process_message(self, level: Text, message: Text, data: Dict) -> Text:
        when = TimeUtils.utc_now().strftime('%m/%d/%Y %H:%M:%S')
        level = level.upper()[0]

        if data:
            data = self._json.decode(self._json.encode(data))
            if self._style == 'json':
                dumped_data = self._to_json(data)
            elif self._style == 'yaml':
                dumped_data = self._to_yaml(data)
            else:
                raise ValueError(f'unrcognized log style: {self.style}')
        else:
            dumped_data = None

        thread = StringUtils.dash(current_thread().name)

        display_string = (
            f'{when} ({level}) {self._name}'
            f' - {message}'
        )
        if dumped_data:
            display_string += f'\n\n{dumped_data}\n'
        return display_string

    def _to_json(self, data):
        return json.dumps(data, indent=2, sort_keys=True)

    def _to_yaml(self, data):
        lines = yaml.dump(data, default_flow_style=False).split('\n')
        return '\n'.join('  ' + line for line in lines)
