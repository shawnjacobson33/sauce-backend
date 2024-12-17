from typing import Optional

import redis


class SNOIDManager:
    """
    Manages a Redis-backed set of unmapped identifiers and entity-domain associations.

    This class provides methods to interact with a Redis set that stores
    entity-domain mappings in the format `{domain}:{entity}`. It is useful
    for tracking entities that are not yet mapped or require further processing.

    Attributes:
        __r (redis.Redis): The Redis client used for data storage.
        snoid (str): The Redis key representing the set of unmapped identifiers.
    """
    def __init__(self, r: redis.Redis, name: str):
        """
        Initialize the SNOIDManager with a Redis client and a base name.

        Args:
            r (redis.Redis): A Redis client instance for interacting with the database.
            name (str): The base name to create the Redis key for storing unmapped identifiers.
        """
        self.__r = r
        self.snoid = f'{name}:noid'

    def getnoid(self) -> Optional[set[str]]:
        """
        Retrieve the set of unmapped identifiers.

        Returns:
            Optional[set[str]]: A set of unmapped identifiers if they exist, otherwise None.
        """
        return self.__r.smembers(self.snoid)

    def getnoids(self) -> Optional[set[str]]:
        """
        Retrieve a set of unmapped entities.

        Returns:
            Optional[set[str]]: A set of unmapped entity identifiers.
        """
        return self.__r.smembers(self.snoid)

    def store(self, domain: str, entity: str) -> None:
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
        self.__r.sadd(self.snoid, f'{domain}:{entity}')