from typing import Any, Optional

import redis

from app.data_storage.managers import LIVEManager
from app.data_storage.stores.base import StaticDataStore


class DynamicDataStore(StaticDataStore):
    def __init__(self, r: redis.Redis, name: str):
        super().__init__(r, name)
        self.live_mngr = LIVEManager(r, name)

    def get_live_entities(self, league: str) -> Optional[dict[str, Any]]:
        if g_ids := self.live_mngr.getgameids(league):
            for g_id in g_ids: yield self._r.hgetall(g_id)



