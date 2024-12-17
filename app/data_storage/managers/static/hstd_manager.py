from typing import Optional

import redis

from app.data_storage.models import AttrEntity, Entity
from app.data_storage.managers import AutoIdManager


class HSTDManager:
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
        """
        Initializes the StaticHSTDManager instance.

        Args:
            r (redis.Redis): A Redis connection instance.
            name (str): The name to associate with this manager's static data store.
        """
        self.__r = r
        self.hstd = f'{name}:hstd'

        # Initialize the AutoIdManager for managing unique entity IDs.
        self.aid_mngr = AutoIdManager(r, self.hstd)

        self.performed_insert = False
        self.updates = 0

    def set_name(self, domain: str) -> str:
        """
        Sets the Redis key for hashed static data to include the specific domain.

        This method modifies the base `hstd` key to incorporate the given domain,
        allowing for domain-specific data storage and retrieval in Redis.

        Args:
            domain (str): The domain to append to the hashed static data key.

        Returns:
            str: Returns the updated hstd name. It updates the `hstd` key to reflect the domain.
        """
        if self.hstd.count(':') == 2:
            self.hstd = f'{self.hstd}:{domain}'
        else:
            partial_name = ':'.join(self.hstd.split(':')[:-1])
            self.hstd = f'{partial_name}:{domain}'

        return self.hstd

    def search_hstd(self, entity: AttrEntity) -> Optional[str]:
        """
        Searches the hash structure for the entity's standardized name and attempts to link
        the entity's name to the same auto-generated ID if found.

        Returns:
            Optional[str]: The auto-generated ID if the entity's standardized name exists,
                           otherwise None.
        """
        if e_id := self.__r.hget(self.hstd, entity.std_name):
            self.updates = self.__r.hsetnx(self.hstd, key=entity.name, value=e_id)
            return e_id

    def add_to_hstd(self, entity: Entity) -> Optional[str]:
        raise NotImplementedError("Must implement!")
