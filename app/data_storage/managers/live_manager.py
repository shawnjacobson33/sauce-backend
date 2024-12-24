from typing import Iterable

import redis

from app.data_storage.managers.manager import Manager
from app.data_storage.managers import GTManager


class LIVEManager(Manager):
    """
    Manages live game data within a Redis database.

    This class provides methods to manage and track live game identifiers for a specific domain.
    It utilizes the `GTManager` to handle time-based storage and retrieval of game identifiers.
    """
    def __init__(self, r: redis.Redis, name: str):
        """
        Initialize the LIVEManager.

        Args:
            r (redis.Redis): The Redis client for database operations.
            name (str): The base name for identifying the keys in Redis.
        """
        super().__init__(r, f'{name}:live')
        self.gt_mngr = GTManager(r, name)

    def getgameids(self, domain: str) -> str:
        """
        Retrieve active game identifiers for a specific domain.

        This method fetches active game IDs from the `GTManager`, stores them in Redis,
        and yields each live game identifier.

        Args:
            domain (str): The domain (e.g., league name) to retrieve active game IDs for.

        Yields:
            str: A live game identifier for the given domain.
        """
        live_ids = self.gt_mngr.getactive(domain)
        self.name = domain
        self.store(live_ids)
        for live_id in live_ids: yield live_id

    def track(self, domain: str, key: str, gt: str) -> None:
        """
        Track a game by storing its game time in the `GTManager`.

        Args:
            domain (str): The domain (e.g., league name) to associate the game with.
            key (str): The identifier of the game.
            gt (int): The game time to be stored, typically as a Unix timestamp.
        """
        self.gt_mngr.store(domain, key, int(gt.timestamp()))

    def store(self, domain: str, l_ids: list[str], l_entities: Iterable) -> None:
        """
        Store a set of live game identifiers in Redis.

        This method adds the provided live game IDs to a Redis set identified by the domain name.

        Args:
            domain (str): The domain (e.g., league name) to store the live game IDs for.
            l_ids (list[str]): A list of live game identifiers to store.
            l_entities (Iterable): An iterable of live game entities to store.
        """
        self.name = domain
        with self._r.pipeline() as pipe:
            pipe.watch(self.name)
            pipe.multi()
            for l_id, l_entity in zip(l_ids, l_entities):
                pipe.zadd(self.name, mapping={ l_id: l_entity.game_time })
                # Todo: needs continually updating of time score trackers as box scores are collected
            pipe.execute()