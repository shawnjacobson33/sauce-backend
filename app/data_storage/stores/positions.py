from typing import Optional, Iterable

import redis

from app.data_storage.models import Position
from app.data_storage.stores.base import StaticDataStore


class Positions(StaticDataStore):
    """
    A Redis-backed manager for handling sports-related position data.

    This class extends `L1StaticDataStore` to provide functionality for managing
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

    def getpos(self, sport: str, pos: str, report: bool = False) -> Optional[str]:
        """
        Retrieve the unique identifier for a specific position in a given sport.

        Args:
            sport (str): The name of the sport (e.g., "basketball", "soccer").
            pos (str): The name of the position to retrieve (e.g., "goalkeeper", "center").
            report (bool, optional): Whether to log the retrieval operation. Defaults to False.

        Returns:
            Optional[str]: The unique identifier for the position if found, otherwise None.
        """
        return self.get_entity('direct', pos, sport, report=report)

    def getpositions(self, sport: str) -> Iterable:
        """
        Retrieve all position identifiers for a given sport.

        Args:
            sport (str): The name of the sport (e.g., "football", "tennis").

        Returns:
            Optional[list[str]]: A list of position identifiers for the sport if any exist,
                                 otherwise None.
        """
        yield from self.get_entities(domain=sport)

    def store(self, sport: str, positions: list[Position]) -> None:
        """
        Store a list of position data for a given sport in the Redis data store.

        Each position is associated with the sport and stored using both its name
        and standardized name for easy retrieval and lookup.

        Args:
            sport (str): The name of the sport (e.g., "baseball", "hockey").
            positions (list[Position]): A list of `Position` objects to be stored.

        Raises:
            AssertionError: If the positions list is empty.
            AttributeError: If any position object lacks required attributes.
        """
        assert positions, f"The list of {self.name} cannot be empty!"
        try:
            self.lookup_mngr.name = sport
            with self._r.pipeline() as pipe:
                pipe.multi()
                for entity in positions:
                    for entity_name in {entity.name, entity.std_name}:
                        pipe.hsetnx(self.lookup_mngr.name, key=entity_name, value=entity.std_name)

                pipe.execute()

        except AttributeError as e:
            self._log_error(e)