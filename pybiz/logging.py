from __future__ import absolute_import

import json
import logging
import yaml

from logging import Formatter, StreamHandler, DEBUG

from appyratus.utils import TimeUtils

from pybiz.util.json_encoder import JsonEncoder


class Logger(object):

    json = JsonEncoder()

    def __init__(self, name, level=None, fstr=None, handlers=None):
        self._name = name
        self._level = level or DEBUG
        self._fstr = fstr

        self._py_logger = logging.getLogger(self._name)
        self._py_logger.setLevel(self._level)

        if handlers:
            self._handlers = handlers
        else:
            stderr_handler = StreamHandler()
            stderr_handler.setLevel(self._level)
            self._handlers = [stderr_handler]

        if self._fstr is None:
            self._fstr = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'

        self._formatter = Formatter(self._fstr)

        for handler in self._handlers:
            if not handler.formatter:
                handler.setFormatter(self._formatter)
            self._py_logger.addHandler(handler)

    def debug(self, message=None):
        self._py_logger.debug(message)

    def info(self, message=None):
        self._py_logger.info(message)

    def warning(self, message=None):
        self._py_logger.warning(message)

    def critical(self, message=None):
        self._py_logger.critical(message)

    def error(self, message=None):
        self._py_logger.critical(message)

    def exception(self, message=None):
        self._py_logger.exception(message)


class ConsoleLogger(Logger):

    def __init__(self, name, level=None, style=None):
        super().__init__(name, level=level, fstr='%(message)s')
        self._style = style or 'json'

    def debug(self, message, **payload):
        logger = self._name
        level = 'DEBUG'
        when = TimeUtils.utc_now().strftime('%m/%d/%Y @ %H:%M:%S')
        payload = self._dump_payload(payload).strip()
        display = (
            f'{{{when}, {level}, {logger}, "{message}"}}\n\n{payload}\n'
        )
        self._py_logger.debug(display)

    def _dump_payload(self, payload):
        if payload:
            payload = self.json.decode(self.json.encode(payload))
            if self._style == 'json':
                return self._to_json(payload)
            elif self._style == 'yaml':
                return self._to_yaml(payload)
            else:
                raise ValueError(f'unrcognized log style: {self.style}')
        return None

    def _to_json(self, payload):
        return json.dumps(payload, indent=2, sort_keys=True)

    def _to_yaml(self, payload):
        return yaml.dump(
            payload, default_flow_style=False, default_style=''
        )


if __name__ == '__main__':
    log = ConsoleLogger(__name__, style='yaml')
    log.debug(
        message='user tried to hack us',
        user={'id': 'a1fb78', 'email': 'foo@bar.baz'}
    )
