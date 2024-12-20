from typing import Optional, Any

import redis

from app.data_storage.models import Entity
from app.data_storage.stores.base import L2DataStore
from app.data_storage.managers import LIVEManager


class L2DynamicDataStore(L2DataStore):
    def __init__(self, r: redis.Redis, name: str):
        super().__init__(r, name)
        self.live_mngr = LIVEManager(self._r, name)

    def _eval_entity(self, entity: Entity, keys: tuple[Any, ...]) -> Optional[str]:
        """
        Evaluate an entity by checking its existence in hashed static data.
        If not found, add the entity to the hashed static data.

        Args:
            entity (namedtuple): The entity to evaluate.

        Returns:
            Optional[str]: The identifier of the entity if found or created, otherwise None.
        """
        self.std_mngr.set_name(entity.domain)
        if e_id := self.std_mngr.search(entity):
            return e_id

        e_id = self.std_mngr.insert(entity, *keys)
        return e_id

