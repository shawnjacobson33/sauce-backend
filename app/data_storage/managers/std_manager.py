from collections import defaultdict
from typing import Optional, Callable, Iterable

import redis

from app.data_storage.models import Entity
from app.data_storage.managers import IDManager
from app.data_storage.managers.manager import Manager
from app.data_storage.stores.utils import get_entity_type


class STDManager(Manager):
    def __init__(self, r: redis.Redis, name: str, id_mngr: IDManager):
        super().__init__(r, f'{name}:std')
        self.id_mngr = id_mngr

    def get_eid(self, domain: str, key: str) -> Optional[str]:
        self.name = domain
        return self._r.hget(self.name, key)

    def _scan_keys(self) -> Iterable:
        e_ids_counter = defaultdict(int)
        for std_key in self._r.hscan_iter(self.name, no_values=True):
            e_id = self._r.hget(self.name, std_key)
            if not e_ids_counter[e_id]:
                e_ids_counter[e_id] += 1
                yield e_id

    def get_eids(self, domain: str = None) -> Iterable:
        if not domain:
            entity_type = get_entity_type(self.name)
            return (t_key for t_key in self._r.scan_iter(f'{entity_type}:*'))

        self.name = domain

        yield from self._scan_keys()

    def _find_eid(self, entity: Entity, keys: Callable) -> Optional[str]:
        e_id = None
        for key in keys(entity):
            if match := self._r.hget(self.name, key):
                e_id = match
                continue
                
            if e_id: self._r.hsetnx(self.name, key=key, value=e_id)
        
        return e_id
    
    def _map_entity(self, entity: Entity, keys: Callable) -> Optional[str]:
        e_id = self.id_mngr.generate()
        for key in keys(entity):
            self.performed_insert = True if self._r.hsetnx(self.name, key=key, value=e_id) else False

        return e_id if self.performed_insert else self.id_mngr.decr()

    def store_eids(self, domain: str, entities: list[Entity], keys: Callable) -> Iterable:
        self.name = domain
        for entity in entities:
            if not (e_id := self._find_eid(entity, keys)):
                e_id = self._map_entity(entity, keys)
            
            yield e_id, entity
        
