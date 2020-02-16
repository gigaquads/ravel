from typing import Text, Dict


class OrderBy(object):
    def __init__(self, key: Text, desc=False):
        self.key = key
        self.desc = desc

    def __repr__(self):
        return (
            f'{get_class_name(self)}('
            f'key={self.key}, '
            f'desc={self.desc}'
            f')'
        )

    def dump(self):
        return {'key': self.key, 'desc': self.desc}

    @classmethod
    def load(cls, data: Dict) -> 'OrderBy':
        return cls(data['key'], data['desc'])

    @property
    def asc(self) -> bool:
        return not self.desc
