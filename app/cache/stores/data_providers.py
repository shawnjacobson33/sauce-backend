from typing import Union, Optional

import redis

from app.cache.stores.base import DataStore


class DataProviders(DataStore):
    def __init__(self, r: redis.Redis):
        super().__init__(r, 'providers')

    def getprovider(self, provider_name: str, key: str = None) -> Optional[Union[str, dict]]:
        return self._r.hget(f'{self.info_name}:{provider_name}', key) if key \
            else self._r.hgetall(f'{self.info_name}:{provider_name}')

    def storeprovider(self, providers: list[dict]) -> None:
        try:
            with self._r.pipeline() as pipe:
                pipe.multi()
                for provider in providers:
                    pipe.hset(f'{self.info_name}:{provider['name']}', mapping=provider)
                pipe.execute()

        except KeyError as e:
            self._log_error(e)