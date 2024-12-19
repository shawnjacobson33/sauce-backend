from typing import Optional

import redis

from app.data_storage.stores.base import L2DataStore
from app.data_storage.managers import LIVEManager


class L2DynamicDataStore(L2DataStore):
    def __init__(self, r: redis.Redis, name: str):
        super().__init__(r, name)
        self.live_mngr = LIVEManager(self._r, name)

