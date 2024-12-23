from typing import Any, Optional, Iterable

import redis

from app.data_storage.managers import LIVEManager
from app.data_storage.stores.base import StaticDataStore


class DynamicDataStore(StaticDataStore):
    """
    A class to manage dynamic data storage by combining functionality from
    StaticDataStore and LIVEManager.

    This store provides mechanisms to interact with live data entities and game IDs
    associated with a specific league.
    """
    def __init__(self, r: redis.Redis, name: str):
        """
        Initialize the DynamicDataStore with a Redis client and a name identifier.

        Args:
            r (redis.Redis): The Redis client instance.
            name (str): The name identifier for this data store.
        """
        super().__init__(r, name)
        self.live_mngr = LIVEManager(r, name)

    def get_live_entities(self, league: str) -> Optional[Iterable]:
        """
        Retrieve live entities for a given league.

        This method fetches game IDs for the specified league using the LIVEManager
        and yields all data associated with each game ID from the Redis store.

        Args:
            league (str): The league identifier to retrieve live entities for.

        Yields:
            Optional[dict[str, Any]]: A dictionary containing live entity data for
            each game ID, or None if no game IDs are found.
        """
        for g_id in self.live_mngr.getgameids(league): yield self._r.hgetall(g_id)

