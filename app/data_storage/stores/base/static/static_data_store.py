import redis

from app.data_storage.managers import SNOIDManager, HSTDManager


class StaticDataStore:
    """
    A base class for managing static data storage in Redis.

    This class facilitates the evaluation, storage, and management of entities in a Redis data store.
    It interacts with Redis through the use of two managers: `SNOIDManager` and `StaticHSTDManager`,
    to handle specific aspects of static data, including entity IDs and hashed static data.

    Attributes:
        __r (redis.Redis): Redis connection instance used for data storage.
        snoid_mngr (SNOIDManager): Manager for handling missing entity IDs (NOIDs).
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