from typing import Optional

import redis

from app.data_storage.models import AttrEntity, Entity
from app.data_storage.managers import Manager, IDManager


class STDManager(Manager):
    """
    A manager class for handling static hashed standardization maps in Redis.

    Inherits from `STDManager` to manage the operations of interacting with
    hashed static data storage in Redis. This manager is specialized to work
    with static data, including inserting and updating entities.

    Attributes:
        id_mngr (IDManager): Manager for handling entity IDs.
    """
    def __init__(self, r: redis.Redis, name: str, id_mngr: IDManager):
        super().__init__(r, f'{name}:std')
        self.id_mngr = id_mngr

    def search(self, entity: AttrEntity) -> Optional[str]:
        """
        Searches the hash structure for the entity's standardized name and attempts to link
        the entity's name to the same auto-generated ID if found.

        Returns:
            Optional[str]: The auto-generated ID if the entity's standardized name exists,
                           otherwise None.
        """
        if e_id := self._r.hget(self.name, entity.std_name):
            self.updates = self._r.hsetnx(self.name, key=entity.name, value=e_id)
            return e_id

    def insert(self, entity: Entity) -> Optional[str]:
       raise NotImplementedError("Must implement!")