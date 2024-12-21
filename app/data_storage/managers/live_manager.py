import redis

from app.data_storage.managers.manager import Manager
from app.data_storage.managers import GTManager


class LIVEManager(Manager):
    def __init__(self, r: redis.Redis, name: str):
        super().__init__(r, f'{name}:live')
        self.gt_mngr = GTManager(r, name)

    def getgameids(self, domain: str) -> str:
        live_ids = self.gt_mngr.getactive(domain)
        self.name = domain
        self.store(live_ids)
        for live_id in live_ids: yield live_id

    def track_game(self, domain: str, key: str, gt) -> None:
        self.gt_mngr.store(domain, key, gt)

    def store(self, live_ids: set[str]) -> None:
        with self._r.pipeline() as pipe:
            pipe.watch(self.name)
            pipe.multi()
            for idx in live_ids:
                pipe.sadd(self.name, idx)
            pipe.execute()