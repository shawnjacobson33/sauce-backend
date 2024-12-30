from typing import Optional, Iterable

import redis
from redis.client import Pipeline

from app.cache.models import League
from app.cache.stores.base import DataStore


class Leagues(DataStore):
    def __init__(self, r: redis.Redis):
        super().__init__(r, 'leagues')

    def getleague(self, league: str, report: bool = True) -> Optional[str]:
        return self._r.hget(self.lookup_name, league)

    def getleagues(self) -> Iterable:
        yield from self._r.hscan_iter(self.lookup_name)

    def _store_in_lookup(self, pipe: Pipeline, league: League) -> None:
        for league_name in {league.name, league.std_name}:
            pipe.hset(self.lookup_name, key=league_name, value=league.std_name)

    def storeleagues(self, leagues: list[League]) -> None:
        try:
            with self._r.pipeline() as pipe:
                pipe.multi()
                for league in leagues:
                    self._store_in_lookup(pipe, league)
                pipe.execute()

        except AttributeError as e:
            self._log_error(e)