from typing import Optional, Iterable

import redis

from app.data_storage.models import Position
from app.data_storage.stores import L1StaticDataStore


class Positions(L1StaticDataStore):
    """
    A Redis-backed manager for handling sports-related position data.

    This class extends `L1StaticDataStore` to provide functionality for managing
    positions in a sports context, such as retrieving specific positions, fetching all positions,
    and storing position data in a structured way.

    Attributes:
        __r (redis.Redis): The Redis client used for data storage and retrieval.
        name (str): The base name used to create Redis keys for storing position data.
    """
    def __init__(self, r: redis.Redis, name: str):
        """
        Initialize the Positions manager with a Redis client and a base name.

        Args:
            r (redis.Redis): A Redis client instance for interacting with the database.
            name (str): The base name to create Redis keys for managing position data.
        """
        super().__init__(r, name)

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
        return self.getentity(sport, pos, report=report)

    def getpositions(self, sport: str) -> Iterable:
        """
        Retrieve all position identifiers for a given sport.

        Args:
            sport (str): The name of the sport (e.g., "football", "tennis").

        Returns:
            Optional[list[str]]: A list of position identifiers for the sport if any exist,
                                 otherwise None.
        """
        yield from self.getentities(sport)

    def store(self, sport: str, positions: list[Position]) -> None:
        """
        Store a list of positions for a given sport in the data store.

        Args:
            sport (str): The name of the sport (e.g., "baseball", "hockey").
            positions (list[Position]): A list of `Position` objects representing the positions
                                         to be stored.

        Returns:
            None: This method does not return a value.
        """
        self._store(sport, positions)