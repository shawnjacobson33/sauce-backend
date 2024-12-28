import json
from typing import Iterable

import redis

from app.data_storage.stores.base import DynamicDataStore


class BettingLines(DynamicDataStore):
    def __init__(self, r: redis.Redis):
        super().__init__(r, 'lines')

    def getids(self, game_id: str) -> Iterable:
        yield from self._r.hscan_iter(f'{self.name}:info:{game_id}', no_values=True)

    def getlines(self, game_id: str) -> Iterable:
        yield from self._r.hscan_iter(f'{self.name}:info:{game_id}')

    def labellines(self, game_id: str, box_score: dict) -> Iterable:
        try:
            for line_id in self.getids(game_id):
                if line_json := self._r.hget(f'{self.name}:info:{game_id}', line_id):
                    line_dict = json.loads(line_json)
                    line_dict['result'] = box_score[line_dict['market']]

                    self._r.hdel(f'{self.name}:info:{game_id}', game_id)
                    yield line_dict

        except KeyError as e:
            self._log_error(e)

    @staticmethod
    def _get_key(line: dict):
        return f'{line['league']}:{line['market']}:{line['subj_id']}'

    def storelines(self, lines: list[dict]) -> None:
        try:
            with self._r.pipeline() as pipe:
                pipe.multi()
                for line in lines:
                    pipe.hset(f'{self.name}:info:{line['game_id']}', self._get_key(line),
                              json.dumps(line))
                pipe.execute()

        except KeyError as e:
            self._log_error(e)
