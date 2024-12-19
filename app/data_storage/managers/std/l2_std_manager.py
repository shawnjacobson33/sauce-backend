from typing import Optional

import redis

from app.data_storage.models import AttrEntity
from app.data_storage.managers.base import IDManager, STDManager


class L2STDManager(STDManager):
    """
    A manager class for handling static hashed standardization maps in Redis.

    Inherits from `STDManager` to manage the operations of interacting with
    hashed static data storage in Redis. This manager is specialized to work
    with static data, including inserting and updating entities.
    """
    def __init__(self, r: redis.Redis, name: str, id_mngr: IDManager):
        super().__init__(r, name, id_mngr)

    def insert(self, entity: AttrEntity, *keys) -> Optional[str]:
        """
        Adds the current entity to the hash structure, generating a new auto ID if necessary.
        Links both the entity's name and standardized name to the same ID.

        Returns:
            Optional[str]: The auto-generated ID if the insertion was successful, otherwise None.
        """
        e_id = self.id_mngr.generate()
        for entity_key in keys:
            if self._r.hsetnx(self.name, key=entity_key, value=e_id):
                self.performed_insert = True

        return e_id if self.performed_insert else self.id_mngr.decrement()