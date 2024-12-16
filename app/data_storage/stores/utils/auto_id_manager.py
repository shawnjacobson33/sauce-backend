import redis

from app.data_storage.stores.utils import get_type


class AutoIdManager:
    """
    Manages the automatic generation, resetting, and decrementing of IDs
    in a Redis-backed system.

    The class uses a Redis instance to maintain and manipulate an auto-incremented
    ID associated with a given name. It supports resetting the ID counter,
    decrementing it, and generating new IDs in a consistent format.

    Attributes:
        __r (redis.Redis): The Redis instance used for managing the ID storage.
        name (str): The unique key in Redis for tracking the auto-incremented ID.
    """
    def __init__(self, r: redis.Redis, name: str):
        """
        Initializes the AutoIdManager with a Redis instance and a name.

        Args:
            r (redis.Redis): A Redis connection instance for ID management.
            name (str): The base name for the ID key in Redis. The name must end with 's'.

        Raises:
            AssertionError: If the provided name does not end with 's'.
        """
        self.__r = r

        assert name and name[-1] == 's'
        self.name = f'{name}:auto:id'

    def reset(self) -> None:
        """
        Resets the ID counter in Redis to 0.

        This method sets the value of the Redis key associated with `self.name` to 0.
        """
        self.__r.set(self.name, 0)

    def decrement(self) -> None:
        """
        Decrements the ID counter in Redis by 1.

        This method reduces the value of the Redis key associated with `self.name`
        by 1, ensuring the counter remains consistent.
        """
        self.__r.decrby(self.name)

    def generate(self) -> str:
        """
        Generates a new ID by incrementing the counter in Redis.

        The new ID is returned as a string in the format '<name_prefix>:<id>',
        where `name_prefix` is derived from the base name without the trailing 's'.

        Returns:
            str: The newly generated ID.
        """
        new_id = self.__r.incrby(self.name)
        return f'{get_type(self.name)}:{new_id}'