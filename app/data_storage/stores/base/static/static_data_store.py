from typing import Optional

import redis

from app.data_storage.models import Entity
from app.data_storage.stores.managers import SNOIDManager, StaticHSTDManager


class StaticDataStore:
    """
    A base class for managing static data storage in Redis.

    This class facilitates the evaluation, storage, and management of entities in a Redis data store.
    It interacts with Redis through the use of two managers: `SNOIDManager` and `StaticHSTDManager`,
    to handle specific aspects of static data, including entity IDs and hashed static data.

    Attributes:
        __r (redis.Redis): Redis connection instance used for data storage.
        snoid_mngr (SNOIDManager): Manager for handling missing entity IDs (NOIDs).
        hstd_mngr (StaticHSTDManager): Manager for handling hashed static data.
        name (str): The name associated with the static data store.
    """
    def __init__(self, r: redis.Redis, name: str):
        """
        Initializes the StaticDataStore instance.

        Args:
            r (redis.Redis): A Redis connection instance.
            name (str): The name to associate with this data store.
        """
        self.__r = r
        self.name = name

        self.snoid_mngr = SNOIDManager(self.__r, name)
        self.hstd_mngr = StaticHSTDManager(self.__r, name)


    def _eval_entity(self, entity: Entity) -> Optional[str]:
        """
        Evaluate an entity by checking its existence in hashed static data.
        If not found, add the entity to the hashed static data.

        Args:
            entity (namedtuple): The entity to evaluate.

        Returns:
            Optional[str]: The identifier of the entity if found or created, otherwise None.
        """
        if e_id := self.hstd_mngr.search_hstd(entity):
            return e_id

        e_id = self.hstd_mngr.add_to_hstd(entity)
        return e_id

    def _log_error(self, e: Exception) -> None:
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