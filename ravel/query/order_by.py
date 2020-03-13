from typing import Text, Dict, List, Union
from datetime import datetime, date

from ravel.util.misc_functions import get_class_name, normalize_to_tuple


class OrderBy(object):
    def __init__(self, key: Text, desc=False):
        self.key = key
        self.desc = desc

    def __repr__(self):
        return (
            f'{get_class_name(self)}('
            f'{self.key} {"desc" if self.desc else "asc"}'
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

    @staticmethod
    def sort(
        resources: List['Resource'],
        order_by: Union['OrderBy', List['OrderBy']]
    ) -> List['Resource']:
        """
        Perform a multi-key sort on the given resource list. This procedure
        approximately O(N log N).
        """
        order_by = normalize_to_tuple(order_by)

        # create functions for converting types that are not inherently
        # sortable to an integer value (which is sortable)
        converters = {
            datetime: lambda x: x.timestamp(),
            date: lambda x: x.toordinal(),
            str: lambda x: int.from_bytes(x.encode('utf-8'), byteorder='big'),
            bytes: lambda x: int.from_bytes(x, byteorder='big'),
        }

        # pre-compute the "index" keys by which the resources shall be sorted.
        # Each index is an array of ints.
        indexes = {}
        for resource in resources:
            index = []
            for x in order_by:
                value = resource.internal.state.get(x.key)
                if value is None:
                    value = 0
                if x.desc:
                    convert = converters.get(type(value))
                    if convert:
                        value = convert(value)
                    index.append(-1 * value)
                else:
                    index.append(value)

            indexes[resource] = tuple(index)

        return sorted(resources, key=lambda x: indexes[x])
