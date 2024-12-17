import redis

from app.data_storage.stores.managers.base import AutoIdManager


class HSTDManager:
    """
    A manager class for handling hashed standardized maps (HSTD) in Redis.

    This class manages operations related to hashed static data storage in Redis,
    including the management of unique identifiers for entities within specific domains.

    Attributes:
        __r (redis.Redis): The Redis connection instance used for data operations.
        hstd (str): The Redis key for hashed static data, incorporating the name of the store.
        aid_mngr (AutoIdManager): A manager responsible for generating and managing unique IDs for entities.
    """
    def __init__(self, r: redis.Redis, name: str):
        """
        Initializes the HSTDManager instance.

        Args:
            r (redis.Redis): A Redis connection instance.
            name (str): The base name to be used for constructing Redis keys related to hashed static data.
        """
        self.__r = r
        self.hstd = f'{name}:hstd'

        # Initialize the AutoIdManager for managing unique entity IDs.
        self.aid_mngr = AutoIdManager(r, self.hstd)

    def set_name(self, domain: str) -> str:
        """
        Sets the Redis key for hashed static data to include the specific domain.

        This method modifies the base `hstd` key to incorporate the given domain,
        allowing for domain-specific data storage and retrieval in Redis.

        Args:
            domain (str): The domain to append to the hashed static data key.

        Returns:
            str: Returns the updated hstd name. It updates the `hstd` key to reflect the domain.
        """
        if self.hstd.count(':') == 2:
            self.hstd = f'{self.hstd}:{domain}'
        else:
            partial_name = ':'.join(self.hstd.split(':')[:-1])
            self.hstd = f'{partial_name}:{domain}'

        return self.hstd
