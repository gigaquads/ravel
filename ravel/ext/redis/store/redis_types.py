from uuid import UUID
from datetime import datetime
from threading import local
from types import GeneratorType

from redis import StrictRedis
from appyratus.utils.time_utils import TimeUtils


class RedisClient(StrictRedis):
    pass


class RedisObject(object):
    def __init__(self, redis, name):
        self._redis = redis
        self._name = name

    @property
    def redis(self):
        return self._redis

    @property
    def name(self):
        return self._name


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

    def increment(self, key, delta=1, pipe=None):
        redis = pipe if pipe is not None else self.redis
        return redis.hincrby(self.name, key, delta)

    def delete(self, key, pipe=None) -> bool:
        redis = pipe if pipe is not None else self.redis
        return bool(redis.hdel(self.name, key))

    def delete_many(self, keys, pipe=None) -> int:
        if isinstance(keys, GeneratorType):
            keys = tuple(keys)
        if keys:
            redis = pipe if pipe is not None else self.redis
            return redis.hdel(self.name, *keys)
        return

    def keys(self, pipe=None):
        redis = pipe if pipe is not None else self.redis
        return (k.decode() for k in redis.hkeys(self.name))

    def values(self, pipe=None):
        redis = pipe if pipe is not None else self.redis
        return (v.decode() for v in redis.hvals(self.name))

    def items(self, pipe=None):
        redis = pipe if pipe is not None else self.redis
        return zip(self.keys(pipe=pipe), self.values(pipe=pipe))

    def update(self, mapping, pipe=None):
        redis = pipe if pipe is not None else self.redis
        if mapping:
            redis.hmset(self.name, mapping)

    def get(self, key, default=None, pipe=None):
        redis = pipe if pipe is not None else self.redis
        return redis.hget(self.name, key) or default

    def get_many(self, keys, pipe=None):
        if isinstance(keys, GeneratorType):
            keys = tuple(keys)
        if keys:
            redis = pipe if pipe is not None else self.redis
            return redis.hmget(self.name, *keys)
        else:
            return []

    def get_all(self):
        return self.redis.hgetall(self.name)


class RangeIndex(RedisObject):

    DELIM = '\0\0'
    DELIM_BYTES = DELIM.encode()

    def upsert(self, _id, value, pipe=None):
        raise NotImplementedError('override in subclass')

    def delete(self, _id, pipe=None):
        raise NotImplementedError('override in subclass')

    def delete_many(self, _ids, pipe=None):
        raise NotImplementedError('override in subclass')

    def search(
        self,
        lower=None,
        upper=None,
        include_lower=True,
        include_upper=False,
        offset=None,
        limit=None,
        pipe=None,
    ):
        raise NotImplementedError('override in subclass')


class NumericIndex(RangeIndex):
    custom_serializers = {
        UUID: lambda x: int(x.hex, 16),
        datetime: lambda x: TimeUtils.to_timestamp(x),
        bool: lambda x: int(x),
    }

    def upsert(self, _id, value, pipe=None):
        redis = pipe if pipe is not None else self.redis
        ser = self.custom_serializers.get(value.__class__)
        value = ser(value) if ser else value

        redis.zrem(self.name, _id)
        redis.zadd(self.name, {_id: value})

    def delete(self, _id, pipe=None):
        redis = pipe if pipe is not None else self.redis
        redis.zrem(self.name, _id)

    def delete_many(self, _ids, pipe=None):
        if isinstance(_ids, GeneratorType):
            _ids = tuple(_ids)
        if _ids:
            redis = pipe if pipe is not None else self.redis
            redis.zrem(self.name, *_ids)

    def search(
        self,
        lower=None,
        upper=None,
        include_lower=True,
        include_upper=False,
        offset=None,
        limit=None,
        pipe=None
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

        redis = pipe if pipe is not None else self.redis

        return [
            v.split(self.DELIM_BYTES)[-1]
            for v in redis.zrangebyscore(
                self.name, lower, upper, start=offset, num=limit
            )
        ]


class StringIndex(RangeIndex):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.lutab = HashSet(self.redis, ':'.join([self.name, 'lutab']))

    def upsert(self, _id, value, pipe=None):
        redis = pipe if pipe is not None else self.redis
        old_key = self.lutab.get(_id)
        new_key = '{}{}{}'.format(value, self.DELIM, _id)

        if old_key is not None:
            redis.zrem(self.name, old_key)

        # TODO: Do this in a pipeline
        redis.zadd(self.name, {new_key: 0.0})
        self.lutab[_id] = new_key

    def delete(self, _id, pipe=None):
        old_key = self.lutab.get(_id)
        if old_key is not None:
            redis = pipe if pipe is not None else self.redis
            self.lutab.delete(old_key)
            redis.zrem(self.name, old_key)

    def delete_many(self, _ids, pipe=None):
        old_keys = self.lutab.get_many(_ids)
        if old_keys:
            redis = pipe if pipe is not None else self.redis
            self.lutab.delete_many(old_keys)
            redis.zrem(self.name, *old_keys)

    def search(
        self,
        lower=None,
        upper=None,
        include_lower=False,
        include_upper=True,
        offset=None,
        limit=None,
        pipe=None,
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

        redis = pipe if pipe is not None else self.redis
        values = redis.zrangebylex(
            self.name, lower, upper, start=offset, num=limit
        )

        return [
            v.split(self.DELIM_BYTES)[-1] for v in values
        ]
