import json
from typing import Callable, Union, Optional

import redis

from app.cache.stores.base import DataStore


class DataProviders(DataStore):
    def __init__(self, r: redis.Redis):
        super().__init__(r, 'providers')

    def getprovider(self, provider_name: str, key: str = None) -> Optional[Union[str, dict]]:
        return self._r.hget(f'{self.info_name}:{provider_name}', key) if key \
            else self._r.hgetall(f'{self.info_name}:{provider_name}')

    def _store_data(self, store: str, data: list[dict], key_func: Callable) -> None:
        with self._r.pipeline() as pipe:
            pipe.multi()
            for item in data:
                pipe.hset(store, key_func(item), json.dumps(item))
            pipe.execute()