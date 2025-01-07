from typing import Optional

import redis

from app.cache.stores.utils import get_entity_type


class IDManager:
    """
    A class for managing ID generation, resetting, and retrieval using Redis.

    This class provides functionality to manage counters and sets in Redis,
    enabling the generation of unique identifiers, resetting counters, and
    managing unmapped entities.

    Attributes:
       noid_name (str): A Redis key used to store unmapped identifiers.
    """
    def __init__(self, r: redis.Redis, name: str):
        """
        Initializes an IDManager instance.

        Args:
            r (redis.Redis): The Redis client instance.
            name (str): The base name for the Redis key.
        """
        self._r = r
        self.name = f'{name}:id'
        self.noid_name = '{}:noid'

    def _get_id_prefix(self) -> str | None:
        """
        Retrieve the prefix for the ID based on the entity type.

        The prefix is determined by the entity type (e.g., 'teams', 'subjects').

        Returns:
            str | None: The prefix for the ID ('t' for teams, 's' for subjects).

        Raises:
            ValueError: If the entity type is invalid.
        """
        entity_type = get_entity_type(self.name)
        if entity_type == 'teams':
            return 't'
        elif entity_type == 'subjects':
            return 's'
        elif entity_type == 'games':
            return 'g'

        raise ValueError(f"Invalid entity type: {entity_type}")

    def generate(self) -> str:

        """
        Generates a new ID by incrementing the counter in Redis.

        The new ID is returned as a string in the format '<name_prefix>:<id>',
        where `name_prefix` is derived from the base name without the trailing 's'.

        Returns:
            str: The newly generated ID.
        """
        new_id = self._r.incrby(self.name)
        return f'{self._get_id_prefix()}{new_id}'

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