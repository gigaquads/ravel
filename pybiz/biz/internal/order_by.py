from typing import Text


class OrderBy(object):
    def __init__(self, key: Text, desc=False):
        self.key = key
        self.desc = desc

    def __repr__(self):
        return f'<OrderBy({self.key} {"desc" if self.desc else "asc"})>'

    def dump(self):
        return {'key': self.key, 'desc': self.desc}

    @classmethod
    def load(cls, data):
        return cls(data['key'], data['desc'])

    @property
    def asc(self):
        return not self.desc
