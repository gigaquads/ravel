class Enum(tuple):
    """
    This implementation of Enum is derived from `tuple` so it can be used as
    an argument to functions that expect a simple sequence of values.
    Internally, the key-value pairs are kept in a auxiliary dict, which
    provides O(1) lookup in place of the tuple's O(N) lookup.
    """

    @classmethod
    def of_strings(cls, *keys, name=None):
        """
        Return an enum where the keys and values are mapped 1-1.

        Args:
            - `keys`: a sequence of strings.
        """
        value_map = {}

        for key in keys:
            assert isinstance(key, str)
            value_map[key.lower()] = key

        return cls(value_map, name=name)

    def __new__(cls, value_map: dict=None, name=None, **value_map_kwargs):
        values = set(value_map.values()) | set(value_map_kwargs.values())
        return super().__new__(cls, values)

    def __init__(self, value_map: dict=None, name=None, **value_map_kwargs):
        super().__init__()
        self._name = name
        self._value_map = value_map or {}
        self._value_map.update(value_map_kwargs)

    def __getattr__(self, key: str):
        if key.startswith('__'):
            raise AttributeError(key)
        return self._value_map[key.lower()]

    def __getitem__(self, key: str):
        return self._value_map[key.lower()]

    def __contains__(self, key: str):
        return key in self._value_map.values()

    @property
    def name(self):
        return self._name
