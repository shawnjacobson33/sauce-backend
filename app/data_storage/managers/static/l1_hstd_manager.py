from typing import Optional

import redis

from app.data_storage.models import AttrEntity
from app.data_storage.managers.static import HSTDManager


class L1HSTDManager(HSTDManager):
    def __init__(self, r: redis.Redis, name: str):
        super().__init__(r, name)

    # TODO: Yet to Implement
    def add_to_hstd(self, entity: AttrEntity) -> Optional[str]:
        e_id = self.aid_mngr.generate()
        for entity_name in {entity.name, entity.std_name}:
            if self.__r.hsetnx(self.hstd, key=entity_name, value=e_id):
                self.performed_insert = True

        return e_id if self.performed_insert else self.aid_mngr.decrement()