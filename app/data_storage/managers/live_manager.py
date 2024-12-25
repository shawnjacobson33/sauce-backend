from datetime import datetime
from typing import Iterable

import redis

from app.data_storage.managers.manager import Manager


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

    def _get_live_ids(self) -> Iterable[str]:
        curr_ts = int(datetime.now().timestamp())
        with self._r.pipeline() as pipe:
            pipe.watch(self.name)
            pipe.multi()
            pipe.zrange(
                name=self.name,
                start=-10000000000,
                end=curr_ts,
                byscore=True
            )
            pipe.zremrangebyscore(
                name=self.name,
                min='-inf',
                max=curr_ts
            )
            live_ids, _ = pipe.execute()
            return live_ids

    def get_and_store_live_game_ids(self, domain: str) -> str:
        """
        Retrieve active game identifiers for a specific domain.

        This method fetches active game IDs from the `GTManager`, stores them in Redis,
        and yields each live game identifier.

        Args:
            domain (str): The domain (e.g., league name) to retrieve active game IDs for.

        Yields:
            str: A live game identifier for the given domain.
        """
        self.name = domain
        if live_ids := self._get_live_ids():
            self.store_live_ids(domain, live_ids)
            for live_id in live_ids: yield live_id
        else:
            print(f'No live games found for {domain}.')

    def store_live_ids(self, domain: str, l_ids: Iterable[str]) -> None:
        """
        Store a set of live game identifiers in Redis.

        This method adds the provided live game IDs to a Redis set identified by the domain name.

        Args:
            domain (str): The domain (e.g., league name) to store the live game IDs for.
            l_ids (list[str]): A list of live game identifiers to store.
        """
        self.name = domain
        with self._r.pipeline() as pipe:
            pipe.watch(self.name)
            pipe.multi()
            for l_id in l_ids:
                if game_time := self._r.hget(l_id, 'game_time'):
                    game_datetime = datetime.strptime(game_time, '%Y-%m-%d %H:%M:%S')
                    pipe.zadd(self.name, mapping={ l_id: int(game_datetime.timestamp()) })

                # Todo: needs continually updating of time score trackers as box scores are collected
            pipe.execute()