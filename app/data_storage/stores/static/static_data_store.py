from typing import Iterable, Optional, Union

import redis
from app.data_storage.stores.base import DataStore


class StaticDataStore(DataStore):
    def __init__(self, r: redis.Redis, name: str):
        super().__init__(r, name)

    def _direct_index(self, key: str, domain: str = None, report: bool = False) -> Optional[str]:
        self.std_mngr.name = domain
        if entity_match := self._r.hget(self.std_mngr.name, key):
            return entity_match

        if report:
            self.id_mngr.storenoid(domain, key)

    def _secondary_index(self, domain: str, key: str, item: str = None, report: bool = False) -> Optional[
        Union[dict[str, str], str]]:
        if e_id := self.std_mngr.get_eid(domain, key):
            return self._r.hgetall(e_id) if not item else self._r.hget(e_id, key=item)

        if report:
            self.id_mngr.storenoid(domain, key)

    def get_entity(self, method: str, *args, **kwargs) -> Optional[str]:
        if method == 'direct': return self._direct_index(*args, **kwargs)
        if method == 'secondary': return self._secondary_index(*args, **kwargs)

    def get_entities(self, domain: str = None) -> Iterable:
        self.std_mngr.name = domain
        yield from self._r.hscan_iter(self.std_mngr.name)
    
    def _handle_error(self, e: Exception) -> None:
        self._log_error(e)
        self.id_mngr.decr()