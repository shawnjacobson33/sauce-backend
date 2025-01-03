import json
from typing import Union, Optional

import redis

from app.cache.stores.base import DataStore


class DataProviders(DataStore):
    def __init__(self, r: redis.Redis):
        super().__init__(r, 'providers')

    def getprovider(self, provider_name: str) -> Optional[Union[str, dict]]:
        if provider_data := self._r.hget(self.info_name, provider_name):
            return json.loads(provider_data)

    def updateprovider(self, provider_name: str, key: str, value: Union[str, dict]) -> None:
        if provider := self.getprovider(provider_name):
            provider[key] = value

            self._r.hset(self.info_name, provider_name, json.dumps(provider))

    def storeproviders(self, providers: list[dict]) -> None:
        try:
            with self._r.pipeline() as pipe:
                pipe.multi()
                for provider in providers:
                    pipe.hset(self.info_name, key=provider['name'], value=json.dumps(provider))
                pipe.execute()

        except KeyError as e:
            self._log_error(e)