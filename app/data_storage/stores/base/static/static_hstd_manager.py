from collections import namedtuple
from typing import Optional

import redis

from app.data_storage.models import Entity
from app.data_storage.stores.utils.auto_id_manager import AutoIdManager


class StaticHSTDManager:
    """
    Manages operations on a static hash structure in Redis, with functionality
    for searching and adding entities while handling auto-generated IDs.

    Attributes:
        r (redis.Redis): Redis client instance for interacting with the Redis database.
        hstd (str): The name of the hash structure in Redis.
        aid (AutoId): An instance of the AutoId class for generating unique IDs.
        entity (namedtuple): The current entity being processed, expected to have
            'name' and 'std_name' attributes.
        performed_insert (bool): Flag indicating if an insertion was performed.
        updates (int): Tracks the number of update operations performed.
    """
    def __init__(self, r: redis.Redis, hstd: str):
        """
        Initializes the StaticHSTDManager with a Redis client and a hash structure name.

        Args:
            r (redis.Redis): Redis client instance.
            hstd (str): Name of the hash structure in Redis to operate on.
        """
        self.r = r
        self.hstd = hstd

        self.aid = AutoIdManager(r, hstd)

        self.performed_insert = False
        self.updates = 0

    def search_hstd(self, entity: Entity) -> Optional[str]:
        """
        Searches the hash structure for the entity's standardized name and attempts to link
        the entity's name to the same auto-generated ID if found.

        Returns:
            Optional[str]: The auto-generated ID if the entity's standardized name exists,
                           otherwise None.
        """
        if a_id := self.r.hget(self.hstd, entity.std_name):
            self.updates = self.r.hsetnx(self.hstd, key=entity.name, value=a_id)
            return a_id

    def add_to_hstd(self, entity: Entity) -> Optional[str]:
        """
        Adds the current entity to the hash structure, generating a new auto ID if necessary.
        Links both the entity's name and standardized name to the same ID.

        Returns:
            Optional[str]: The auto-generated ID if the insertion was successful, otherwise None.
        """
        a_id = self.aid.generate()
        for entity_name in {entity.name, entity.std_name}:
            if self.r.hsetnx(self.hstd, key=entity_name, value=a_id):
                self.performed_insert = True

        return a_id if self.performed_insert else self.aid.decrement()
