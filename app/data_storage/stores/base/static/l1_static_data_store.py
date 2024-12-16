from typing import Optional, Any

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
        if std_entity := self.__r.hget(self._hstd.format(domain), entity):
            return std_entity

        if report:
            self._set_noid(domain, entity)
            
    def getentities(self, domain: str) -> Any:
        """
        Retrieves all standardized entities for a given partition.

        Args:
            domain (str): The partition or category to retrieve entities from.

        Returns:
            Any: A collection of standardized entity names from the partition.
        """
        return self.__r.hgetall(self._hstd.format(domain)).values()

    def store(self, domain: str, entities: list[Entity]):
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
        self._set_hstd(domain)
        try:
            with self.__r.pipeline() as pipe:
                pipe.multi()
                for entity in entities:
                    for entity_name in {entity.name, entity.std_name}:
                        pipe.hsetnx(self._hstd, key=entity_name, value=entity.std_name)

                pipe.execute()

        except AttributeError as e:
            self._attr_error_cleanup(e)