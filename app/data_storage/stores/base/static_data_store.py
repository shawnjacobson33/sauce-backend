from typing import Iterable, Optional, Union

import redis
from app.data_storage.stores.base import DataStore


class StaticDataStore(DataStore):
    """
    A class to manage static data storage and retrieval in a Redis-based system.

    Inherits from the DataStore class to utilize Redis-based data management features
    for static data, supporting direct and secondary indexing methods for efficient data lookup.
    """
    def __init__(self, r: redis.Redis, name: str):
        """
        Initializes the StaticDataStore class with a Redis client instance and a store name.

        Args:
            r (redis.Redis): The Redis client used to interact with the Redis database.
            name (str): The name of the data store or collection to be used in Redis.
        """
        super().__init__(r, name)

    def _direct_index_every(self, domain: str) -> Iterable:
        self.std_mngr.name = domain
        yield from self._r.hscan_iter(self.std_mngr.name)

    def _direct_index(self, key: str, domain: str = None, every: bool = False, report: bool = False) -> Optional[str]:
        """
        Retrieves an entity directly using a key and domain from the Redis store.

        Args:
            key (str): The key associated with the entity to retrieve.
            domain (str, optional): The domain used to identify the store. Defaults to None.
            every (bool, optional): Whether to retrieve all entities in the store. Defaults to False.
            report (bool, optional): Whether to store the entity ID if not found. Defaults to False.

        Returns:
            Optional[str]: The entity's data if found, otherwise None.

        If the entity is not found and the `report` argument is True, the entity ID is stored.
        """
        self.std_mngr.name = domain
        if every:
            yield from self._direct_index_every(domain)
        else:
            if entity_match := self._r.hget(self.std_mngr.name, key):
                return entity_match

        if report: self.id_mngr.storenoid(domain, key)

    def _secondary_index_every(self, domain: str) -> Iterable:
        if e_ids := self.std_mngr.get_eids(domain):
            for e_id in e_ids: yield self._r.hgetall(e_id)

    def _secondary_index(self, domain: str, key: str = None, item: str = None, every: bool = False, report: bool = False) -> \
        Optional[Iterable]:
        """
        Retrieves an entity via a secondary index, which may return either an entire entity or a specific item.

        Args:
            domain (str): The domain used to identify the store.
            key (str): The key associated with the entity to retrieve.
            item (str, optional): A specific item within the entity to retrieve. Defaults to None.
            every (bool, optional): Whether to retrieve all entities in the store. Defaults to False.
            report (bool, optional): Whether to store the entity ID if not found. Defaults to False.

        Returns:
            Optional[Iterable]: The entity's data if found, otherwise None.

        If the entity is not found and the `report` argument is True, the entity ID is stored.
        """
        if every:
            yield from self._secondary_index_every(domain)
        else:
            if key:
                if e_id := self.std_mngr.get_eid(domain, key):
                    yield self._r.hgetall(e_id) if not item else self._r.hget(e_id, key=item)

                if report: self.id_mngr.storenoid(domain, key)
            else:
                raise ValueError("A key must be provided to retrieve an entity.")


    def get_entity(self, method: str, *args, **kwargs) -> Optional[Union[str, dict]]:
        """
        Retrieves an entity based on the specified method (either 'direct' or 'secondary').

        Args:
            method (str): The method to use for retrieving the entity ('direct' or 'secondary').
            *args: Arguments to pass to the respective indexing method.
            **kwargs: Additional keyword arguments to pass to the respective indexing method.

        Returns:
            Optional[Union[str, dict]]: The entity's data if found, otherwise None.
        """
        if method == 'direct': return next(iter(self._direct_index(*args, **kwargs)), None)
        if method == 'secondary': return next(iter(self._secondary_index(*args, **kwargs)), None)

    def get_entities(self, method: str, *args, **kwargs) -> Iterable:
        """
        Retrieves all entities in the store for a given domain.

        Args:
            method (str): The method to use for retrieving the entities ('direct' or 'secondary').
            *args: Arguments to pass to the respective indexing method.
            **kwargs: Additional keyword arguments to pass to the respective indexing method.

        Yields:
            Iterable: A generator yielding key-value pairs for each entity in the specified domain.
        """
        if method == 'direct': yield from self._direct_index(*args, every=True, **kwargs)
        if method == 'secondary': yield from self._secondary_index(*args, every=True, **kwargs)
    
    def _handle_error(self, e: Exception) -> None:
        """
        Handles an error by logging it and decrementing the error counter.

        Args:
            e (Exception): The exception to handle.
        """
        self._log_error(e)
        self.id_mngr.decr()