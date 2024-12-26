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

    def getbkm(self, bookmaker: str) -> Optional[str]:
        """
        Retrieve the unique identifier for a specific bookmaker.

        Args:
            bookmaker (str): The name of the bookmaker (e.g., "Bet365", "DraftKings").

        Returns:
            Optional[str]: The unique identifier for the bookmaker if found, otherwise None.
        """
        return self.get_entity('direct', bookmaker)

    def getbookmakers(self):
        """
        Retrieve all bookmaker identifiers from the data store.

        Returns:
            Iterable: A generator yielding all bookmaker identifiers.
        """
        return self.get_entities()

    def store(self, bookmakers: list[Bookmaker]):
        """
        Store a list of bookmakers in the Redis data store.

        Each bookmaker is stored with its name as the key and its default odds as the value.

        Args:
            bookmakers (list[Bookmaker]): A list of `Bookmaker` objects to be stored.

        Raises:
            AssertionError: If the bookmakers list is empty.
            AttributeError: If any bookmaker object lacks required attributes.
        """
        assert bookmakers, f"The {self.name} list cannot be empty!"
        try:
            with self._r.pipeline() as pipe:
                pipe.multi()
                for bkm in bookmakers:
                    pipe.hsetnx(self.lookup_mngr.name, key=bkm.name, value=str(bkm.dflt_odds))

                pipe.execute()

        except AttributeError as e:
            self._log_error(e)