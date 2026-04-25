"""In-memory Redis mock for local dev when Redis is unavailable. NOT for production."""
from loguru import logger


class MockRedis:
    def __init__(self):
        self._data = {}
        logger.warning("Using In-Memory Mock Redis. DO NOT USE IN PRODUCTION.")

    async def get(self, key):
        return self._data.get(key)

    async def set(self, key, value, ex=None, nx=False):
        if nx and key in self._data:
            return None
        self._data[key] = value
        return True

    async def incr(self, key):
        val = int(self._data.get(key, 0)) + 1
        self._data[key] = str(val)
        return val

    async def decr(self, key):
        val = int(self._data.get(key, 0)) - 1
        self._data[key] = str(val)
        return val

    async def rpush(self, key, *values):
        lst = self._data.get(key) if isinstance(self._data.get(key), list) else []
        lst.extend(str(v) for v in values)
        self._data[key] = lst
        return len(lst)

    async def lpush(self, key, *values):
        lst = self._data.get(key) if isinstance(self._data.get(key), list) else []
        for v in reversed(values):
            lst.insert(0, str(v))
        self._data[key] = lst
        return len(lst)

    async def lpop(self, key):
        lst = self._data.get(key)
        if not lst or not isinstance(lst, list):
            return None
        val = lst.pop(0)
        self._data[key] = lst
        return val

    async def llen(self, key):
        lst = self._data.get(key)
        return len(lst) if isinstance(lst, list) else 0

    async def setex(self, key, ttl, value):
        self._data[key] = value

    async def delete(self, key):
        self._data.pop(key, None)

    async def scard(self, key):
        s = self._data.get(key)
        return len(s) if isinstance(s, set) else 0

    async def sadd(self, key, *values):
        if not isinstance(self._data.get(key), set):
            self._data[key] = set()
        before = len(self._data[key])
        self._data[key].update(str(v) for v in values)
        return len(self._data[key]) - before

    async def srem(self, key, *values):
        if not isinstance(self._data.get(key), set):
            return 0
        removed = sum(1 for v in values if str(v) in self._data[key])
        self._data[key].difference_update(str(v) for v in values)
        return removed

    async def smembers(self, key):
        s = self._data.get(key)
        return set(s) if isinstance(s, set) else set()

    async def expire(self, key, seconds):
        return 1 if key in self._data else 0

    async def eval(self, script, numkeys, *keys_and_args):
        logger.warning("MockRedis.eval() — Lua not supported in mock, returning 1")
        return 1

    async def evalsha(self, sha, numkeys, *keys_and_args):
        logger.warning("MockRedis.evalsha() — Lua not supported in mock, returning 1")
        return 1

    async def ping(self):
        return True

    async def close(self):
        pass

    @classmethod
    def from_url(cls, url, decode_responses=True):
        return cls()
