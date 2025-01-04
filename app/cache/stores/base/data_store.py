import redis

from app.cache.stores.base import IDManager


class DataStore:
    def __init__(self, r: redis.Redis, name: str):
        self._r = r
        self.name = name

        self.lookup_name = f'{name}:lookup'
        self.info_name = f'{name}:info'
        # Initialize the IdManager for managing unique entity IDs.
        self.id_mngr = IDManager(r, name)

    def _log_error(self, e: Exception) -> None:
        print(f"[{self.name.title()}]: ERROR --> {e}")

    def _handle_error(self, e: Exception) -> None:
        """
        Handles an error by logging it and decrementing the error counter.

        Args:
            e (Exception): The exception to handle.
        """
        self._log_error(e)
        self.id_mngr.decr()