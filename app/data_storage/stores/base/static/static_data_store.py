from collections import namedtuple
from typing import Optional

import redis

from app.data_storage.models import Entity
from app.data_storage.stores.base.static import StaticHSTDManager


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

        self.name = name

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

    def _set_noid(self, domain: str, entity: str) -> None:
        """
        Adds a combination of partition and entity to a Redis set.

        This method constructs a string in the format "{partition}:{entity}"
        and adds it to a Redis set identified by `self._snoid`.

        Args:
            domain (str): The name of the partition (e.g., a namespace or category).
            entity (str): The name of the entity to associate with the partition.

        Returns:
            None: This method does not return a value.
        """
        self.__r.sadd(self._snoid, f'{domain}:{entity}')
    
    def _set_hstd(self, domain: str) -> None:
        self._hstd.format(domain)
        
    def _eval_entity(self, entity: Entity) -> Optional[str]:
        """
        Evaluate an entity by checking its existence in hashed static data.
        If not found, add the entity to the hashed static data.

        Args:
            entity (namedtuple): The entity to evaluate.

        Returns:
            Optional[str]: The identifier of the entity if found or created, otherwise None.
        """
        self._hstd_manager.hstd = self._hstd.format(entity.domain)
        if e_id := self._hstd_manager.search_hstd(entity):
            return e_id

        e_id = self._hstd_manager.add_to_hstd(entity)
        return e_id

    def _attr_error_cleanup(self, e: AttributeError) -> None:
        """
        Handles error cleanup for attribute-related operations.

        This method logs an error message with the current instance's name
        and performs cleanup actions by decrementing a counter in the associated
        `aid` object of the `_hstd_manager`.

        Args:
            e (AttributeError): The error message to be logged and displayed.

        Returns:
            None: This method does not return a value.
        """
        print(f"[{self.name.title()}]: ERROR --> {e}")
        self._hstd_manager.aid.decrement()