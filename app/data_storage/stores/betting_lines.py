from typing import Iterable

import redis

from app.data_storage.stores import Subjects
from app.data_storage.stores.base import DynamicDataStore


class BettingLines(DynamicDataStore):
    def __init__(self, r: redis.Redis):
        super().__init__(r, 'betting_lines')

    def getbettinglines(self, league: str, is_live: bool = False) -> Iterable:
        yield from (self.get_live_entities(league) if is_live else self.get_entities('secondary', league))

    @staticmethod
    def get_key(betting_line: dict):
        return f"{betting_line['league']}:{betting_line['market']}:{betting_line['s_id']}"

    def store(self, league: str, betting_lines: list[dict]) -> None:
        with self._r.pipeline() as pipe:
            pipe.multi()
            for b_id, betting_line in self.std_mngr.store_eids(league, betting_lines, BettingLines.get_key):
                pipe.hset(b_id, mapping=betting_line)
                Subjects.setactive(self._r, betting_line['s_id'])  # Set this subject as active so redis can reduce the number of box scores it stores
                self.live_mngr.track(league, b_id, betting_line['game_time'])
            pipe.execute()
