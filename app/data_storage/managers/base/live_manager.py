from typing import Optional

import redis

from app.data_storage.managers.manager import Manager
from app.data_storage.managers.base import GTManager


class LIVEManager(Manager):
    def __init__(self, r: redis.Redis, name: str):
        super().__init__(r, f'{name}:live')
        self.gt_mngr = GTManager(r, name)

    def getgameids(self, league: str) -> Optional[set[str]]:
        live_ids = self.gt_mngr.getactive(league)
        self.set_name(league)
        self.store(live_ids)
        return live_ids

    def store(self, live_ids: set[str]) -> None:
        with self._r.pipeline() as pipe:
            pipe.watch(self.name)
            pipe.multi()
            for idx in live_ids:
                pipe.sadd(self.name, idx)
            pipe.execute()