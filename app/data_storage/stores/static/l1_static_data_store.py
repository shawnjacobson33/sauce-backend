from typing import Optional, Iterable

import redis

from app.data_storage.managers import L1STDManager
from app.data_storage.stores.base import DataStore


class L1StaticDataStore(DataStore):
    """
    A class that manages static data for entities stored in a Redis database.

    This class extends `DataStore` to provide specialized methods for
    storing and retrieving standardized entity names and their mappings.
    """
    def __init__(self, r: redis.Redis, name: str):
        """
        Initializes the SimpleDataStore instance.

        Args:
            r (redis.Redis): A Redis connection instance.
            name (str): The name of the data store.
        """
        super().__init__(r, name)
        self.std_mngr = L1STDManager(self._r, name)
    
    def getentity(self, entity: str, domain: str = None, report: bool = False) -> Optional[str]:
        """
        Retrieves the standardized name of an entity.

        Args:
            domain (str): The partition or category the entity belongs to.
            entity (str): The name of the entity to retrieve.
            report (bool, optional): If True, reports and logs non-standard entities. Default is False.

        Returns:
            Optional[str]: The standardized entity name if found, otherwise None.
        """
        std_name = self.std_mngr.set_name(domain) if domain else self.std_mngr.name
        if entity_match := self._r.hget(std_name, entity):
            return entity_match

        if report:
            self.id_mngr.storenoid(domain, entity)

    def getentities(self, domain: str = None) -> Iterable:
        """
        Retrieves all standardized entities for a given partition.

        Args:
            domain (str): The partition or category to retrieve entities from.

        Returns:
            Any: A collection of standardized entity names from the partition.
        """
        std_name = self.std_mngr.set_name(domain) if domain else self.std_mngr.name
        for val in self._r.hgetall(std_name).values(): yield val
