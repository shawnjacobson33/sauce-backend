from typing import Optional, Union, Iterable, Any

import redis

from app.data_storage.models import Entity
from app.data_storage.managers import L2STDManager
from app.data_storage.stores.base import DataStore
from app.data_storage.stores.utils import get_entity_type


class L2DataStore(DataStore):
    """
    L2DataStore is a data store that provides higher-level storage capabilities for managing
    and interacting with hierarchical structured data.

    Attributes:
        std_mngr (L2STDManager): A manager for handling standardized data (STD).
    """
    def __init__(self, r: redis.Redis, name: str):
        """
        Initialize the L2DataStore instance.

        Args:
            r (redis.Redis): Redis connection instance.
            name (str): Name of the data store.
        """
        super().__init__(r, name)
        self.std_mngr = L2STDManager(self._r, name, self.id_mngr)

    def geteid(self, domain: str, key: str) -> Optional[str]:
        """
        Retrieves the entity ID for a given entity and domain.

        Args:
            domain (str): The domain or partition the entity belongs to.
            key (str): The entity key to retrieve the ID for.

        Returns:
            Optional[str]: The entity ID if found, otherwise None.
        """
        std_name = self.std_mngr.set_name(domain)
        return self._r.hget(std_name, key=key)

    def getentity(self, domain: str, key: str, item: str = None, report: bool = False) -> Optional[
        Union[dict[str, str], str]]:
        """
        Retrieves the data associated with an entity, either the entire entity or a specific field (key).

        Args:
            domain (str): The domain or partition the entity belongs to.
            key (str): The entity id to retrieve.
            item (str, optional): The specific key within the entity to retrieve. Default is None.
            report (bool, optional): If True, will log and report missing entities. Default is False.

        Returns:
            Optional[Union[dict[str, str], str]]: The full entity data as a dictionary, or a specific field value as a string.
        """
        if e_id := self.geteid(domain, key):
            return self._r.hgetall(e_id) if not item else self._r.hget(e_id, key=item)

        if report:
            self.id_mngr.storenoid(domain, key)

    def geteids(self, domain: str = None) -> Iterable:
        """
        Retrieves the IDs of all entities in the specified domain or across all domains.

        Args:
            domain (str, optional): The domain to retrieve entity IDs from. If None, retrieves IDs for all domains.

        Returns:
            Iterable: An iterable of entity IDs.
        """




    def getentities(self, domain: str = None) -> Iterable:
        """
        Retrieves the data for all entities in the specified domain or across all domains.

        Args:
            domain (str, optional): The domain to retrieve entity data from. If None, retrieves data for all domains.

        Returns:
            Iterable: An iterable of entity data (dictionaries).
        """
        if not domain:
            entity_type = get_entity_type(self.name)
            return (self._r.hgetall(t_key) for t_key in self._r.scan_iter(f'{entity_type}:*'))

        std_name = self.std_mngr.set_name(domain)
        for t_id in self._r.hscan_iter(std_name): yield self._r.hgetall(t_id)

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

