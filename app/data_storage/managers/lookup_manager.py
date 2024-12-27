from collections import defaultdict
from typing import Optional, Callable, Iterable

import redis

from app.data_storage.models import Entity
from app.data_storage.managers import IDManager
from app.data_storage.managers.manager import Manager
from app.data_storage.stores.utils import get_entity_type


class LookupManager(Manager):
    """
    STDManager handles the management of standardized entities in a Redis store. It provides methods
    to retrieve, map, and store entity IDs (EIDs) associated with specific domains or keys.

    Attributes:
        id_mngr (IDManager): Manager to handle the generation and decrementing of unique IDs.
    """
    def __init__(self, r: redis.Redis, name: str, id_mngr: IDManager):
        """
        Initialize the STDManager.

        Args:
            r (redis.Redis): Redis client instance for interacting with the database.
            name (str): Name of the Redis keyspace for storing standardized entities.
            id_mngr (IDManager): IDManager instance for generating unique entity IDs.
        """
        super().__init__(r, f'{name}:lookup')
        self.id_mngr = id_mngr

    def get_entity_id(self, domain: str, key: str) -> Optional[str]:
        """
        Retrieve the entity ID (EID) associated with a given key in the specified domain.

        Args:
            domain (str): The domain name in which the key resides.
            key (str): The key for which to retrieve the EID.

        Returns:
            Optional[str]: The entity ID if found, otherwise None.
        """
        self.name = domain
        return self._r.hget(self.name, key)

    def _scan_keys(self) -> Iterable:
        """
        Scan through the keys in the current domain and yield unique entity IDs.

        Returns:
            Iterable: A generator yielding unique entity IDs.
        """
        e_ids_counter = defaultdict(int)
        for std_key in self._r.hscan_iter(self.name, no_values=True):
            e_id = self._r.hget(self.name, std_key)
            if not e_ids_counter[e_id]:
                e_ids_counter[e_id] += 1
                yield e_id

    def get_entity_ids(self, domain: str = None) -> Iterable:
        """
        Retrieve all entity IDs for a specific domain or all domains.

        Args:
            domain (str, optional): The domain for which to retrieve entity IDs.
                                    If None, retrieves IDs for all domains.

        Returns:
            Iterable: A generator yielding entity IDs.
        """
        if not domain:
            entity_type = get_entity_type(self.name)
            return (t_key for t_key in self._r.scan_iter(f'{entity_type}:*'))

        self.name = domain
        yield from self._scan_keys()

    def _find_eid(self, entity: Entity, keys: Callable, expireat: Callable = None) -> Optional[str]:
        """
        Attempt to find an entity ID (EID) for a given entity by checking its keys.

        Args:
            entity (Entity): The entity to look up.
            keys (Callable): A callable that generates keys for the entity.
            expireat (Callable): A callable that generates expiration timestamps for each key.

        Returns:
            Optional[str]: The entity ID if found, otherwise None.
        """
        entity_id = None
        for key in keys(entity):
            if match := self._r.hget(self.name, key):
                entity_id = match
                continue
                
            if entity_id:
                self._r.hsetnx(self.name, key=key, value=entity_id)
                if expireat:
                    self._r.hexpireat(self.name, expireat(entity), key)
        
        return entity_id
    
    def _map_entity(self, entity: Entity, keys: Callable, expireat: Callable = None) -> Optional[str]:
        """
        Map a new entity to a unique ID (EID) and store its keys.

        Args:
            entity (Entity): The entity to map.
            keys (Callable): A callable that generates keys for the entity.
            expireat (Callable): A callable that generates expiration timestamps for each key.

        Returns:
            Optional[str]: The generated EID if the mapping is successful, otherwise None.
        """
        entity_id = self.id_mngr.generate()
        for key in keys(entity):
            self.performed_insert = True if self._r.hset(self.name, key=key, value=entity_id) else False
            if expireat:
                self._r.hexpireat(self.name, expireat(entity), key)

        return entity_id if self.performed_insert else self.id_mngr.decr()

    def store_entity_ids(self, domain: str, entities: list[Entity], keys: Callable, expireat: Callable = None) -> Iterable:
        """
        Store entity IDs (EIDs) for a list of entities in the specified domain.

        Args:
            domain (str): The domain in which to store the entities.
            entities (list[Entity]): A list of entities to store.
            keys (Callable): A callable that generates keys for each entity.
            expireat (Callable): A callable that generates expiration timestamps for each key.

        Yields:
            Tuple[str, Entity]: A tuple of the stored EID and the corresponding entity.
        """
        self.name = domain
        for entity in entities:
            if not (entity_id := self._find_eid(entity, keys, expireat=expireat)):
                entity_id = self._map_entity(entity, keys, expireat=expireat)
            
            yield entity_id, entity
        
