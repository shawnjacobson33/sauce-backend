from typing import Iterable, Callable

import redis
from app.data_storage.models import Entity, AttrEntity
from app.data_storage.stores.base import L2DataStore as L2


class L2StaticDataStore(L2):
    """
    A class that manages static data for entities with an additional layer of entity retrieval and
    organization, using Redis as a storage backend.

    This class extends `DataStore` to provide methods for retrieving entity data based on
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
