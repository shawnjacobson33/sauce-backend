from typing import Optional, Any, Iterable

import redis

from app.data_storage.models import Entity
from app.data_storage.stores.base import StaticDataStore


class L1StaticDataStore(StaticDataStore):
    """
    A class that manages static data for entities stored in a Redis database.

    This class extends `StaticDataStore` to provide specialized methods for
    storing and retrieving standardized entity names and their mappings.
    """
    def __init__(self, r: redis.Redis, name: str):
        """
        Initializes the SimpleStaticDataStore instance.

        Args:
            r (redis.Redis): A Redis connection instance.
            name (str): The name of the data store.
        """
        super().__init__(r, name)
    
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
        hstd_name = self.hstd_mngr.set_name(domain) if domain else self.hstd_mngr.hstd
        if entity_match := self.__r.hget(hstd_name, entity):
            return entity_match

        if report:
            self.snoid_mngr.store(domain, entity)

    def getentities(self, domain: str = None) -> Iterable:
        """
        Retrieves all standardized entities for a given partition.

        Args:
            domain (str): The partition or category to retrieve entities from.

        Returns:
            Any: A collection of standardized entity names from the partition.
        """
        hstd_name = self.hstd_mngr.set_name(domain) if domain else self.hstd_mngr.hstd
        for val in self.__r.hgetall(hstd_name).values(): yield val
