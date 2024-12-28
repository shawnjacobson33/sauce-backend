from typing import Optional

import redis

from app.data_storage.models import Bookmaker
from app.data_storage.stores.base import StaticDataStore


class Bookmakers(StaticDataStore):
    """
    A Redis-backed manager for handling bookmaker data.

    This class extends `L1StaticDataStore` to manage bookmaker-related operations, such as
    retrieving specific bookmakers, fetching all bookmakers, and storing bookmaker data.

    Attributes:
        _r (redis.Redis): The Redis client instance used for interacting with the database.
    """
    def __init__(self, r: redis.Redis):
        """
        Initialize the Bookmakers manager with a Redis client.

        Args:
            r (redis.Redis): A Redis client instance for interacting with the database.
        """
        super().__init__(r, 'bookmakers')

    def getbookmaker(self, bookmaker: str) -> Optional[str]:
        return self._r.hget(f'{self.name}:info', bookmaker)

    def getbookmakers(self):
        yield from self._r.hscan_iter(f'{self.name}:info')

    def storebookmakers(self, bookmakers: list[Bookmaker]):
        try:
            with self._r.pipeline() as pipe:
                pipe.multi()
                for bookmaker in bookmakers:
                    pipe.hset(f'{self.name}:info', key=bookmaker.name, value=str(bookmaker.dflt_odds))
                pipe.execute()

        except AttributeError as e:
            self._log_error(e)