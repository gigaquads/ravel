from typing import Text


class OrderBy(object):
    def __init__(self, key: Text, desc=False):
        self.key = key
        self.desc = desc

    def dump(self):
        return {'key': key, 'desc': desc}

    @classmethod
    def load(self, data):
        return cls(data['key'], data['desc'])

    @property
    def asc(self):
        return not self.desc
