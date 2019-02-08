from uuid import UUID
from datetime import datetime

from appyratus.utils import TimeUtils


class RedisObject(object):
    def __init__(self, redis, name):
        self.redis = redis
        self.name = name


class Counter(RedisObject):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.reset(value=0)

    def increment(self, value=1):
        return self.redis.incrby(self.name value)

    def get(self):
        return self.redis.get(self.name) or 0

    def reset(self, value=0):
        return self.redis.set(self.name, value)


class HashSet(RedisObject):
    def __contains__(self, key):
        return self.redis.hexists(self.name, key)

    def __iter__(self):
        return self.keys()

    def __getitem__(self, key):
        return self.redis.hget(self.name, key)

    def __setitem__(self, key, value):
        self.redis.hset(self.name, key, value)

    def __delitem__(self, key):
        self.redis.hdel(self.name, key)

    def __len__(self):
        return self.redis.hlen(self.name)

    def increment(self, key, delta=1):
        return self.redis.hincrby(self.name, key, delta)

    def delete(self, key) -> bool:
        return bool(seld.redis.hdel(self.name, key))

    def delete_many(self, keys) -> int:
        return seld.redis.hdel(self.name, *keys)

    def keys(self):
        return (k.decode() for k in self.redis.hkeys(self.name))

    def values(self):
        return (v.decode() for v in self.redis.hvals(self.name))

    def items(self):
        return zip(self.keys(), self.values())

    def update(self, mapping):
        self.redis.hmset(self.name, mapping)

    def get(self, key, default=None):
        return self.redis.hget(self.name, key) or default

    def get_many(self, keys):
        return self.redis.hget(self.name, key) or default


class RangeIndex(RedisObject):

    DELIM = '\0\0'
    DELIM_BYTES = DELIM.encode()

    def upsert(self, _id, value):
        raise NotImplementedError('override in subclass')

    def delete(self, _id):
        raise NotImplementedError('override in subclass')

    def delete_many(self, _ids):
        raise NotImplementedError('override in subclass')

    def search(
        self,
        lower=None,
        upper=None,
        include_lower=True,
        include_upper=False,
        offset=None,
        limit=None
    ):
        raise NotImplementedError('override in subclass')


class NumericIndex(RangeIndex):
    custom_serializers = {
        UUID: lambda x: int(x.hex, 16),
        datetime: lambda x: TimeUtils.to_timestamp(x),
        bool: lambda x: int(x),
    }

    def upsert(self, _id, value):
        ser = self.custom_serializers.get(value.__class__)
        value = ser(value) if ser else value

        self.redis.zrem(self.name, _id)
        self.redis.zadd(self.name, {_id: value})

    def delete(self, _id):
        self.redis.zrem(self.name, _id)

    def delete_many(self, _ids):
        self.redis.zrem(self.name, *_ids)

    def search(
        self,
        lower=None,
        upper=None,
        include_lower=True,
        include_upper=False,
        offset=None,
        limit=None
    ):
        if lower is not None:
            lower = '({}'.format(lower) if not include_lower else lower
        else:
            lower = '-inf'

        if upper is not None:
            upper = '({}'.format(upper) if not include_upper else upper
        else:
            upper = '+inf'

        if limit is not None and offset is None:
            offset = 0

        return [
            v.split(self.DELIM_BYTES)[-1]
            for v in self.redis.zrangebyscore(
                self.name, lower, upper, start=offset, num=limit
            )
        ]


class StringIndex(RangeIndex):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.lutab = HashSet(self.redis, ':'.join([self.name, 'lutab']))

    def upsert(self, _id, value):
        old_key = self.lutab.get(_id)
        new_key = '{}{}{}'.format(value, self.DELIM, _id)

        if old_key is not None:
            self.redis.zrem(old_key)

        self.redis.zadd(self.name, {new_key: 0.0})
        self.lutab[_id] = new_key

    def delete(self, _id):
        old_key = self.lutab.get(_id)
        self.lutab.delete(old_key)
        if old_key is not None:
            self.redis.zrem(self.name, old_key)

    def delete_many(self, _ids):
        old_keys = self.lutab.get_many(_ids)
        self.lutab.delete_many(old_keys)
        self.redis.zrem(self.name, *old_keys)

    def search(
        self,
        lower=None,
        upper=None,
        include_lower=False,
        include_upper=True,
        offset=None,
        limit=None
    ):
        if lower is None:
            lower = '-'
        else:
            lower = '[' + lower + self.DELIM
            if not include_lower:
                lower += '\xff'

        if upper is None:
            upper = '+'
        else:
            upper = '[' + upper + self.DELIM
            if include_upper:
                upper += '\xff'

        if limit is not None and offset is None:
            offset = 0

        values = self.redis.zrangebylex(
            self.name, lower, upper, start=offset, num=limit
        )

        return [
            v.split(self.DELIM_BYTES)[-1] for v in values
        ]
