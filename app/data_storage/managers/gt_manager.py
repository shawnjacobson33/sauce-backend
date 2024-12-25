from datetime import datetime
from typing import Optional

import redis

from app.data_storage.managers.manager import Manager


class GTManager(Manager):
    """
    A manager class for handling game timestamps (GT) in Redis. It provides functionality to retrieve
    and store active game IDs based on timestamps.
    """
    def __init__(self, r: redis.Redis, name: str):
        """
        Initialize the GTManager.

        Args:
            r (redis.Redis): Redis client instance.
            name (str): The base name for the GT Redis key.
        """
        super().__init__(r, f'{name}:gt')

    def getliveeids(self, league: str) -> Optional[set[str]]:
        """
        Retrieve active game IDs for a specified league.

        This method fetches game IDs whose timestamps are within a valid range (past to present).
        It also removes expired game IDs from the Redis sorted set.

        Args:
            league (str): The league name for which to retrieve active game IDs.

        Returns:
            Optional[set[str]]: A set of active game IDs or None if no active games exist.
        """
        self.name = league
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

    def store(self, domain: str, key: str, gt: int) -> None:
        """
        Store a game ID with its corresponding timestamp in the Redis sorted set.

        Args:
            domain (str): The domain (league) name to associate with the game ID.
            key (str): The game ID to store.
            gt (float): The game timestamp.

        Returns:
            None
        """
        self.name = domain
        self._r.zadd(self.name, mapping={key: gt})