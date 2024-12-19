import redis

from app.data_storage.managers import IDManager


class DataStore:
    def __init__(self, r: redis.Redis, name: str):
        self._r = r
        self.name = name

        # Initialize the IdManager for managing unique entity IDs.
        self.id_mngr = IDManager(r, name)

    def _log_error(self, e: Exception) -> None:
        print(f"[{self.name.title()}]: ERROR --> {e}")