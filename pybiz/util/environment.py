import os
import re

import pybiz.schema

from pybiz.exc import PyBizError


class EnvironmentError(PyBizError):
    pass


class UndefinedVariableError(EnvironmentError):
    pass


class EnvironmentValidationError(EnvironmentError):
    pass


class Environment(pybiz.schema.Schema):

    _instance = None  # <- the singleton instance
    _re_magic_attr = re.compile(r'^__\w+__$')

    @classmethod
    def get_instance(cls):
        if cls._instance is None:
            cls._instance = cls()

            # remove the fields declared on the class; otherwise, they'll
            # shadow the same-named instance attributes and __getattr__
            # won't work right.
            for k in cls._instance.fields:
                delattr(cls, k)

        return cls._instance

    def __init__(self):
        super().__init__(strict=False, allow_additional=True)
        result = self.load({k.lower(): v for k, v in os.environ.items()})

        if result.errors:
            raise EnvironmentValidationError(result.errors)

        self._data = result.data

    def __repr__(self):
        return '<Environment(singleton)>'

    def __getattr__(self, key):
        if self._re_magic_attr.match(key):
            raise AttributeError(key)
        return self[key]

    def __getitem__(self, key):
        key = key.lower()
        if key not in self._data:
            raise UndefinedVariableError(
                '{} environment variable is undefined'.format(key))

        return self._data[key]

    def __keys__(self):
        return self._data.keys()

    def __values__(self):
        return self._data.values()

    def __items__(self):
        return self._data.items()

    def __contains__(self, key):
        return key in self._data

    def __len__(self):
        return len(self._data)
