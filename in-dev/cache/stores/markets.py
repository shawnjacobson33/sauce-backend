from typing import Optional, Iterable

import redis
from redis.client import Pipeline

from app.cache.models import Market
from app.cache.stores.base import DataStore


class Markets(DataStore):
    def __init__(self, r: redis.Redis):
        super().__init__(r, 'markets')

    def getmarket(self, sport: str, market: str, report: bool = False) -> str | None:
        return self._r.hget(f'{self.lookup_name}:{sport.lower()}', market)

    def getmarkets(self, sport: str) -> Iterable:
        yield from self._r.hscan_iter(f'{self.lookup_name}:{sport.lower()}')

    def _store_in_lookup(self, sport: str, pipe: Pipeline, market: Market) -> None:
        for market_name in {market.name, market.std_name}:
            pipe.hset(f'{self.lookup_name}:{sport.lower()}', key=market_name, value=market.std_name)

    def storemarkets(self, sport: str, markets: list[Market]) -> None:
        try:
            with self._r.pipeline() as pipe:
                pipe.multi()
                for market in markets:
                    self._store_in_lookup(sport, pipe, market)
                pipe.execute()

        except AttributeError as e:
            self._log_error(e)