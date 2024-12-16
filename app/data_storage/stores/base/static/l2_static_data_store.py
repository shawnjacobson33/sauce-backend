from typing import Union, Optional, Iterable

import redis

from app.data_storage.models import Entity
from app.data_storage.stores.utils import get_entity_type
from app.data_storage.stores.base.static import StaticDataStore


class L2StaticDataStore(StaticDataStore):
    """
    A class that manages static data for entities with an additional layer of entity retrieval and
    organization, using Redis as a storage backend.

    This class extends `StaticDataStore` to provide methods for retrieving entity data based on
    domain and entity identifiers, and also supports retrieval by specific keys and bulk retrieval.
    """
    def __init__(self, r: redis.Redis, name: str):
        """
        Initializes the L2StaticDataStore instance.

        Args:
            r (redis.Redis): A Redis connection instance.
            name (str): The name of the data store.
        """
        super().__init__(r, name)
    
    def getentityid(self, domain: str, entity: str) -> Optional[str]:
        """
        Retrieves the entity ID for a given entity and domain.

        Args:
            domain (str): The domain or partition the entity belongs to.
            entity (str): The entity name to retrieve the ID for.

        Returns:
            Optional[str]: The entity ID if found, otherwise None.
        """
        return self.__r.hget(self._hstd.format(domain), key=entity)
    
    def getentity(self, domain: str, entity: str, key: str = None, report: bool = False) -> Optional[
        Union[dict[str, str], str]]:
        """
        Retrieves the data associated with an entity, either the entire entity or a specific field (key).

        Args:
            domain (str): The domain or partition the entity belongs to.
            entity (str): The entity name to retrieve.
            key (str, optional): The specific key within the entity to retrieve. Default is None.
            report (bool, optional): If True, will log and report missing entities. Default is False.

        Returns:
            Optional[Union[dict[str, str], str]]: The full entity data as a dictionary, or a specific field value as a string.
        """
        if e_id := self.getentityid(domain, entity):
            return self.__r.hgetall(e_id) if not key else self.__r.hget(e_id, key=key)
    
        if report:
            self._set_noid(domain, entity)
    
    def getentityids(self, domain: str = None) -> Iterable:
        """
        Retrieves the IDs of all entities in the specified domain or across all domains.

        Args:
            domain (str, optional): The domain to retrieve entity IDs from. If None, retrieves IDs for all domains.

        Returns:
            Iterable: An iterable of entity IDs.
        """
        if not domain:
            entity_type = get_entity_type(self.name)
            return (t_key for t_key in self.__r.scan_iter(f'{entity_type}:*'))

        return (self.__r.hget(self._hstd, t_key) for t_key in self.__r.hscan_iter(self._hstd.format(domain)))
    
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
            return (self.__r.hgetall(t_key) for t_key in self.__r.scan_iter(f'{entity_type}:*'))

        return (self.__r.hgetall(t_id) for t_id in self.__r.hscan_iter(self._hstd.format(domain)))
    
    def _get_eids(self, domain: str, entities: list[Entity]) -> Iterable:
        """
        Retrieves the entity IDs for a list of entities.

        Args:
            domain (str): The domain to look for entities in.
            entities (list[Entity]): A list of `Entity` objects to retrieve IDs for.

        Yields:
            Tuple: Yields a tuple of entity ID and corresponding `Entity` object.

        Raises:
            AssertionError: If the `entities` list is empty.
        """
        assert entities, f"The list of {self.name} cannot be empty!"
        self._set_hstd(domain)
        for entity in entities:
            if e_id := self._eval_entity(entity):
                yield e_id, entity