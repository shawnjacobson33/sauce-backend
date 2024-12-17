from typing import Optional

import redis

from app.data_storage.models import AttrEntity
from app.data_storage.managers.static import HSTDManager


class L2HSTDManager(HSTDManager):
    """
    A manager class for handling static hashed standardization maps in Redis.

    Inherits from `HSTDManager` to manage the operations of interacting with
    hashed static data storage in Redis. This manager is specialized to work
    with static data, including inserting and updating entities.

    Attributes:
        performed_insert (bool): A flag indicating if an insert operation has been performed.
        updates (int): A counter tracking the number of updates performed.
    """
    def __init__(self, r: redis.Redis, name: str):
        super().__init__(r, name)

    def add_to_hstd(self, entity: AttrEntity) -> Optional[str]:
        """
        Adds the current entity to the hash structure, generating a new auto ID if necessary.
        Links both the entity's name and standardized name to the same ID.

        Returns:
            Optional[str]: The auto-generated ID if the insertion was successful, otherwise None.
        """
        e_id = self.aid_mngr.generate()
        for entity_name in {entity.name, entity.std_name}:
            if self.__r.hsetnx(self.hstd, key=entity_name, value=e_id):
                self.performed_insert = True

        return e_id if self.performed_insert else self.aid_mngr.decrement()