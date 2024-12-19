from datetime import datetime
from typing import Optional

import redis

from app.data_storage.managers import Manager
from app.data_storage.stores.utils import convert_to_timestamp


class GTManager(Manager):
    def __init__(self, r: redis.Redis, name: str):
        super().__init__(r, f'{name}:gt')

    def getactive(self, league: str) -> Optional[set[str]]:
        self.set_name(league)
        curr_ts = convert_to_timestamp(datetime.now())
        with self._r.pipeline() as pipe:
            pipe.watch(self.name)
            pipe.multi()
            pipe.zrange(
                name=self.name,
                start=int(float('-inf')),
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

    def store(self, key: str, gt: int) -> None:
        self._r.zadd(self.name, mapping={key: gt})