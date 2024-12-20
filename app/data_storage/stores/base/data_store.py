import redis

from app.data_storage.managers import IDManager, STDManager


class DataStore:
    def __init__(self, r: redis.Redis, name: str):
        self._r = r
        self.name = name

        # Initialize the IdManager for managing unique entity IDs.
        self.id_mngr = IDManager(r, name)
        self.std_mngr = STDManager(r, name, self.id_mngr)

    def _log_error(self, e: Exception) -> None:
        print(f"[{self.name.title()}]: ERROR --> {e}")