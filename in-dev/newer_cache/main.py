import redis.asyncio as redis

from app.cache import stores


class RedisCache:
    def __init__(self):
        self._pool = redis.ConnectionPool.from_url('redis://localhost:6379')
        self.client = redis.Redis.from_pool(self._pool)
        self.betting_lines = stores.BettingLines(self.client)

    async def __aenter(self):
        return self

    async def __aexit__(self):
        await self.client.aclose()


