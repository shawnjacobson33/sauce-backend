from typing import Optional, Iterable

import redis
from redis.client import Pipeline

from app.data_storage.models import Market
from app.data_storage.stores.base import StaticDataStore


class Markets(StaticDataStore):
    """
    A data store class for managing Market entities in a Redis database.
    """
    def __init__(self, r: redis.Redis):
        """
        Initializes the Markets data store.

        Args:
            r (redis.Redis): A Redis client instance.
        """
        super().__init__(r, 'markets')

    def getmarket(self, sport: str, market: str, report: bool = False) -> Optional[str]:
        return self._r.hget(f'{self.name}:lookup:{sport.lower()}', market)

    def getmarkets(self, sport: str) -> Iterable:
        yield from self._r.hscan_iter(f'{self.name}:lookup:{sport.lower()}')

    def _store_in_lookup(self, sport: str, pipe: Pipeline, market: Market) -> None:
        for market_name in {market.name, market.std_name}:
            pipe.hset(f'{self.name}:lookup:{sport.lower()}', key=market_name, value=market.std_name)

    def storemarkets(self, sport: str, markets: list[Market]) -> None:
        try:
            with self._r.pipeline() as pipe:
                pipe.multi()
                for market in markets:
                    self._store_in_lookup(sport, pipe, market)
                pipe.execute()

        except AttributeError as e:
            self._log_error(e)