import redis

from app.cache import stores


class RedisCache:
    def __init__(self):
        self.client = redis.Redis()
        self.betting_lines = stores.BettingLines(self.client)


session = RedisCache()
