from typing import Optional

import redis

from app.data_storage.managers.manager import Manager
from app.data_storage.stores.utils import get_entity_type


class IDManager(Manager):
    def __init__(self, r: redis.Redis, name: str):
        super().__init__(r, f'{name}:id')
        self.noid_name = '{}:noid'

    def generate(self) -> str:

        """
        Generates a new ID by incrementing the counter in Redis.

        The new ID is returned as a string in the format '<name_prefix>:<id>',
        where `name_prefix` is derived from the base name without the trailing 's'.

        Returns:
            str: The newly generated ID.
        """
        new_id = self._r.incrby(self.name)
        return f'{get_entity_type(self.name)}:{new_id}'

    def decr(self) -> None:
        """
        Decrements the ID counter in Redis by 1.

        This method reduces the value of the Redis key associated with `self.name`
        by 1, ensuring the counter remains consistent.
        """
        self._r.decrby(self.name)

    def reset(self) -> None:
        """
        Resets the ID counter in Redis to 0.

        This method sets the value of the Redis key associated with `self.name` to 0.
        """
        self._r.set(self.name, 0)

    def getnoid(self) -> Optional[set[str]]:
        """
        Retrieve the set of unmapped identifiers.

        Returns:
            Optional[set[str]]: A set of unmapped identifiers if they exist, otherwise None.
        """
        return self._r.smembers(self.noid_name)

    def getnoids(self) -> Optional[set[str]]:
        """
        Retrieve a set of unmapped entities.

        Returns:
            Optional[set[str]]: A set of unmapped entity identifiers.
        """
        return self._r.smembers(self.noid_name)

    def storenoid(self, domain: str, entity: str) -> None:
        """
        Adds a combination of partition and entity to a Redis set.

        This method constructs a string in the format "{partition}:{entity}"
        and adds it to a Redis set identified by `self._name`.

        Args:
            domain (str): The name of the partition (e.g., a namespace or category).
            entity (str): The name of the entity to associate with the partition.

        Returns:
            None: This method does not return a value.
        """
        self._r.sadd(self.noid_name, f'{domain}:{entity}')