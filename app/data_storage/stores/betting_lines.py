import json
from typing import Iterable

import redis

from app.data_storage.stores.base import DynamicDataStore


class BettingLines(DynamicDataStore):
    def __init__(self, r: redis.Redis):
        super().__init__(r, 'lines')

    def getlines(self, league: str = None, game_id: str = None) -> Iterable:
        if game_id:
            yield from self._r.sscan_iter(f'lines:index:{game_id}')
        elif league:
            yield from self.get_entities('secondary', league)

        raise ValueError("Either league or game ID must be provided.")

    def labellines(self, game_id: str, box_score: dict) -> Iterable:
        try:
            for line_id in self.getlines(game_id=game_id):
                league = box_score['league']
                if betting_line_json := self._r.hget(f'{self.name}:{league.lower()}', line_id):
                    betting_line_dict = json.loads(betting_line_json)
                    betting_line_dict['result'] = box_score[betting_line_dict['market']]

                    self._r.hdel(f'{self.name}:{league.lower()}', line_id)
                    self._r.hdel(f'{self.name}:index:{game_id}', game_id)
                    self._r.hdel(f'{self.name}:lookup:{league}',
                                 self._get_key(betting_line_dict))

                    yield betting_line_dict

        except KeyError as e:
            print(f'Error evaluating betting lines: {e}')

    @staticmethod
    def _get_key(betting_line: dict):
        return f"{betting_line['league']}:{betting_line['market']}:{betting_line['s_id']}"

    def storelines(self, league: str, betting_lines: list[dict]) -> None:
        try:
            with self._r.pipeline() as pipe:
                pipe.multi()
                for b_id, betting_line in self.lookup_mngr.store_entity_ids(league, betting_lines, self._get_key):
                    pipe.hset(f'{self.name}:{league.lower()}', b_id, json.dumps(betting_line))
                    pipe.sadd(f'{self.name}:index:{betting_line["g_id"]}', b_id)
                pipe.execute()

        except KeyError as e:
            print(f'Error storing betting lines: {e}')
