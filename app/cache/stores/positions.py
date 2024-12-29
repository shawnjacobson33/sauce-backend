from typing import Optional, Iterable

import redis
from redis.client import Pipeline

from app.data_storage.models import Position, Market
from app.data_storage.stores.base import DataStore


class Positions(DataStore):
    """
    A Redis-backed manager for handling sports-related position data.

    This class extends `L1DataStore` to provide functionality for managing
    positions in a sports context, such as retrieving specific positions, fetching all positions,
    and storing position data in a structured way.

    Attributes:
        _r (redis.Redis): The Redis client used for data storage and retrieval.
        name (str): The base name used to create Redis keys for storing position data.
    """
    def __init__(self, r: redis.Redis):
        """
        Initialize the Positions manager with a Redis client and a base name.

        Args:
            r (redis.Redis): A Redis client instance for interacting with the database.
        """
        super().__init__(r, 'positions')

    def getposition(self, sport: str, position: str, report: bool = False) -> Optional[str]:
        return self._r.hget(f'{self.lookup_name}:{sport.lower()}', position)

    def getpositions(self, sport: str) -> Iterable:
        yield from self._r.hscan_iter(f'{self.lookup_name}:{sport.lower()}')

    def _store_in_lookup(self, sport: str, pipe: Pipeline, position: Position) -> None:
        for position_name in {position.name, position.std_name}:
            pipe.hset(f'{self.lookup_name}:{sport.lower()}', key=position_name, value=position.std_name)

    def storepositions(self, sport: str, positions: list[Position]) -> None:
        try:
            with self._r.pipeline() as pipe:
                pipe.multi()
                for position in positions:
                    self._store_in_lookup(sport, pipe, position)
                pipe.execute()

        except AttributeError as e:
            self._log_error(e)