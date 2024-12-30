from typing import Optional

import redis

from app.cache.models import Bookmaker
from app.cache.stores.base import DataStore


class Bookmakers(DataStore):
    def __init__(self, r: redis.Redis):
        super().__init__(r, 'bookmakers')

    def getbookmaker(self, bookmaker: str) -> Optional[str]:
        return self._r.hget(f'{self.info_name}', bookmaker)

    def getbookmakers(self):
        yield from self._r.hscan_iter(f'{self.info_name}')

    def storebookmakers(self, bookmakers: list[Bookmaker]):
        try:
            with self._r.pipeline() as pipe:
                pipe.multi()
                for bookmaker in bookmakers:
                    pipe.hset(f'{self.info_name}', key=bookmaker.name, value=str(bookmaker.dflt_odds))
                pipe.execute()

        except AttributeError as e:
            self._log_error(e)