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
    
    def getentity(self, domain: str, entity: str, report: bool = False) -> Optional[str]:
        """
        Retrieves the standardized name of an entity.

        Args:
            domain (str): The partition or category the entity belongs to.
            entity (str): The name of the entity to retrieve.
            report (bool, optional): If True, reports and logs non-standard entities. Default is False.

        Returns:
            Optional[str]: The standardized entity name if found, otherwise None.
        """
        if std_entity := self.__r.hget(self.hstd_mngr.set_name(domain), entity):
            return std_entity

        if report:
            self.snoid_mngr.store(domain, entity)

    def getentities(self, domain: str) -> Iterable:
        """
        Retrieves all standardized entities for a given partition.

        Args:
            domain (str): The partition or category to retrieve entities from.

        Returns:
            Any: A collection of standardized entity names from the partition.
        """
        for val in self.__r.hgetall(self.hstd_mngr.set_name(domain)).values(): yield val

    def _store(self, domain: str, entities: list[Entity]):
        """
        Stores a list of entities into the data store.

        Args:
            domain (str): in what sport or league does the entity exist
            entities (list[Entity]): A list of `Entity` objects to store in Redis.

        Raises:
            AssertionError: If the `entities` list is empty.
            AttributeError: If an entity's attribute is missing during storage.
        """
        assert entities, f"The list of {self.name} cannot be empty!"
        hstd_name = self.hstd_mngr.set_name(domain)
        try:
            with self.__r.pipeline() as pipe:
                pipe.multi()
                for entity in entities:
                    for entity_name in {entity.name, entity.std_name}:
                        pipe.hsetnx(hstd_name, key=entity_name, value=entity.std_name)

                pipe.execute()

        except AttributeError as e:
            self._attr_error_cleanup(e)