from typing import Optional, Iterable

import redis

from app.data_storage.models import Market
from app.data_storage.stores import L1StaticDataStore


class Markets(L1StaticDataStore):
    """
    A data store class for managing Market entities in a Redis database.
    """
    def __init__(self, r: redis.Redis, name: str):
        """
        Initializes the Markets data store.

        Args:
            r (redis.Redis): A Redis client instance.
            name (str): The name of the data store (e.g., 'markets').
        """
        super().__init__(r, 'markets')

    def getmarket(self, sport: str, market: str, report: bool = False) -> Optional[str]:
        """
        Retrieves the detailed representation of a market for a specific sport.

        Args:
            sport (str): The sport associated with the market.
            market (str): The name of the market to retrieve.
            report (bool, optional): Whether to include reporting information.

        Returns:
            Optional[str]: The market details if found, otherwise `None`.
        """
        return self.getentity(market, domain=sport, report=report)

    def getmarkets(self, sport: str) -> Iterable:
        """
        Retrieves detailed representations of all markets for a specific sport.

        Args:
            sport (str): The sport to filter markets by.

        Yields:
            Iterable: An iterable of detailed market representations.
        """
        yield from self.getentities(domain=sport)

    def store(self, sport: str, markets: list[Market]) -> None:
        """
        Stores a batch of markets in the database for a given sport.

        Args:
            sport (str): The sport associated with the markets.
            markets (list[Market]): A list of Market instances to store.

        Raises:
            AssertionError: If the list of markets is empty.
            AttributeError: If there is an issue with the market attributes or Redis pipeline.
        """
        assert markets, f"The list of {self.name} cannot be empty!"
        try:
            std_name = self.std_mngr.set_name(sport)
            with self._r.pipeline() as pipe:
                pipe.multi()
                for entity in markets:
                    for entity_name in {entity.name, entity.std_name}:
                        pipe.hsetnx(std_name, key=entity_name, value=entity.std_name)

                pipe.execute()

        except AttributeError as e:
            self._log_error(e)