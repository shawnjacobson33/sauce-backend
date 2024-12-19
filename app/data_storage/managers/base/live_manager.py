from typing import Optional

import redis

from app.data_storage.managers import Manager
from app.data_storage.models import AttrEntity, Entity


class LIVEManager(Manager):
    def __init__(self, r: redis.Redis, name: str):
        super().__init__(r, f'{name}:slive')

    def getall(self) -> Optional[str]:
        curr_ts = convert_to_timestamp(datetime.now())
        live_ids = r.zrange(
            name=zwatch,
            start=int(float('-inf')),
            end=curr_ts,
            byscore=True
        )
        r.zrem(zwatch, *live_ids)
        _update_live_set(r, slive, live_ids)
        return live_ids

    def insert(self, entity: Entity) -> Optional[str]:
       raise NotImplementedError("Must implement!")