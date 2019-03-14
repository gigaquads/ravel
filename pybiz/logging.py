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
        self._style = style or 'json'
        super().__init__(
            name,
            level=level,
            fstr='%(levelname)s (%(name)s) @ %(message)s',
        )

    def debug(self, message, **values):
        if self._style == 'json':
            log_message = self._format_json_log_record('DEBUG', message, values)
        elif self._style == 'yaml':
            log_message = self._format_yaml_log_record('DEBUG', message, values)
        self._py_logger.debug(log_message)

    def _format_json_log_record(self, level, message, values):
        record = {
            'message': message,
        }
        if values:
            record['values'] = values

        message = self.json.encode(record)
        safe_record = self.json.decode(message)

        message = json.dumps(safe_record, indent=2, sort_keys=True)
        message = (
            '[{0:%Y-%m-%d %H:%M:%S}]\n'.format(TimeUtils.utc_now())
            + message
        )
        return message

    def _format_yaml_log_record(self, level, message, values):
        record = {
            'message': message,
        }
        if values:
            record['metadata'] = values

        message = self.json.encode(record)
        safe_record = self.json.decode(message)

        message = '{}\n{}'.format(
            '{0:%Y-%m-%d %H:%M:%S}'.format(TimeUtils.utc_now()),
            yaml.dump(
                safe_record,
                explicit_start=True,
                default_flow_style=False,
                default_style=''
            )
        )
        return message


if __name__ == '__main__':
    log = ConsoleLogger(__name__, style='yaml')
    log.debug(
        message='hello, world!',
        user={'id': 'a1fb78', 'email': 'foo@bar.baz'}
    )
