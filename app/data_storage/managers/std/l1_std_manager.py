from typing import Optional

import redis

from app.data_storage.models import AttrEntity
from app.data_storage.managers.base import STDManager, IDManager


class L1STDManager(STDManager):
    def __init__(self, r: redis.Redis, name: str, id_mngr: IDManager):
        super().__init__(r, name, id_mngr)

    # TODO: Yet to Implement
    def insert(self, entity: AttrEntity) -> Optional[str]:
        e_id = self.id_mngr.generate()
        for entity_name in {entity.name, entity.std_name}:
            if self._r.hsetnx(self.name, key=entity_name, value=e_id):
                self.performed_insert = True

        return e_id if self.performed_insert else self.id_mngr.decrement()