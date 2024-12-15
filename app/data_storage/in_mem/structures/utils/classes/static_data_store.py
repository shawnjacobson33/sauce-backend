from collections import namedtuple
from typing import Optional

import redis

from app.data_storage.in_mem.structures.utils.classes.static_hstd_manager import StaticHSTDManager


NAMESPACE_TEMPLATE = {
    'hstd': '{}:std:{}',
    'snoid': '{}:noid',
}


class StaticDataStore:
    """
    A class for managing static data storage in Redis.

    Attributes:
        __r (redis.Redis): Redis client instance.
        _hstd (str): Key template for hashed static data.
        _snoid (str): Key template for unmapped identifiers.
        _hstd_manager (StaticHSTDManager): Manager for hashed static data operations.
        partition (str): Placeholder for a partition identifier.
    """
    def __init__(self, r: redis.Redis, name: str):
        """
        Initialize the StaticDataStore instance.

        Args:
           r (redis.Redis): Redis client instance.
           name (str): Namespace identifier for the Redis keys.
        """
        self.__r = r
        self._hstd = NAMESPACE_TEMPLATE['hstd'].format(name)
        self._snoid = NAMESPACE_TEMPLATE['snoid'].format(name)

        self._hstd_manager = StaticHSTDManager(self.__r, self._hstd)

        self.partition = ''

    def getnoid(self) -> Optional[set[str]]:
        """
        Retrieve the set of unmapped identifiers.

        Returns:
            Optional[set[str]]: A set of unmapped identifiers if they exist, otherwise None.
        """
        return self.__r.smembers(self._snoid)

    def getnoids(self) -> Optional[set[str]]:
        """
        Retrieve a set of unmapped entities.

        Returns:
            Optional[set[str]]: A set of unmapped entity identifiers.
        """
        return self.__r.smembers(self._snoid)

    def _eval_entity(self, entity: namedtuple) -> Optional[str]:
        """
        Evaluate an entity by checking its existence in hashed static data.
        If not found, add the entity to the hashed static data.

        Args:
            entity (namedtuple): The entity to evaluate.

        Returns:
            Optional[str]: The identifier of the entity if found or created, otherwise None.
        """
        self._hstd_manager.entity = entity
        self._hstd_manager.hstd = self._hstd.format(self.partition)
        if entity_id := self._hstd_manager.search_hstd():
            return entity_id

        entity_id = self._hstd_manager.add_to_hstd()
        return entity_id
