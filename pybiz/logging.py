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
            fstr='[%(levelname)s, %(name)s, %(asctime)s]\n ↪ %(message)s',
        )

    def debug(self, message, **values):
        if self._style == 'json':
            log_message = self._format_json('DEBUG', message, values)
        elif self._style == 'yaml':
            log_message = self._format_yaml('DEBUG', message, values)
        else:
            raise ValueError(f'unrcognized log style: {self.style}')
        self._py_logger.debug(log_message)

    def _format_json(self, level, message, values):
        safe_record = self._create_safe_record(values)
        json_str = json.dumps(safe_record, indent=2, sort_keys=True)
        json_str = '\n'.join('  ' + s for s in json_str.split('\n'))
        return '[' + message + ']\n\n' + json_str + '\n'

    def _format_yaml(self, level, message, values):
        safe_record = self._create_safe_record(values)
        yaml_str = yaml.dump(
            safe_record,
            default_flow_style=False,
            default_style=''
        )
        yaml_str = '\n'.join('  ' + s for s in yaml_str.split('\n'))
        return '[' + message + ']\n\n' + yaml_str

    def _create_safe_record(self, values):
        if values:
            json_str = self.json.encode(values)
            return self.json.decode(json_str)
        else:
            return {}


if __name__ == '__main__':
    for style in ['json', 'yaml']:
        log = ConsoleLogger(__name__, style=style)
        log.debug(
            message='user tried to hack us',
            user={'id': 'a1fb78', 'email': 'foo@bar.baz'}
        )
