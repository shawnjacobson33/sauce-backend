from typing import Iterable

import redis

from app.data_storage.stores import Subjects, BoxScores
from app.data_storage.stores.base import DynamicDataStore


class BettingLines(DynamicDataStore):
    def __init__(self, r: redis.Redis, subjects: Subjects, box_scores: BoxScores):
        super().__init__(r, 'betting_lines')
        self.subjects = subjects
        self.box_scores = box_scores

    def _getbettinglinesforgame(self, g_id: str) -> Iterable:
        yield from self._r.sscan_iter(f'betting_lines:idx:{g_id}')

    def getbettinglines(self, league: str, g_id: str = None, is_live: bool = False) -> Iterable:
        if g_id:
            yield from self._getbettinglinesforgame(g_id)
        elif is_live:
            yield from self.get_live_entities(league)
        else:
            self.get_entities('secondary', league)

    def evaluatebettinglines(self, g_ids: list[str]) -> Iterable:
        # will be called when a game is complete to label betting lines in relation to game outcomes.
        # will need to gather all betting lines associated with the game.
        # will call the box scores class to retrieve the box score data for the game.
        try:
            for g_id in g_ids:
                for b_id in self._getbettinglinesforgame(g_id):
                    if betting_line := self._r.hgetall(b_id):
                        if stat := self.box_scores.getboxscore(betting_line['s_id'], stat=betting_line['market']):
                            betting_line['result'] = stat
                            self._r.delete(b_id)
                            yield betting_line
                        else:
                            print(f"No box score data found for {betting_line["s_id"]}'s {betting_line['market']}")

        except KeyError as e:
            print(f'Error evaluating betting lines: {e}')

    def remove(self, b_id: str) -> None:
        # will be called when done evaluating betting lines and then sending them to a transformer before
        # being sent to long-term storage.
        self._r.delete(b_id)

    @staticmethod
    def _get_key(betting_line: dict):
        return f"{betting_line['league']}:{betting_line['market']}:{betting_line['s_id']}"

    def store(self, league: str, betting_lines: list[dict]) -> None:
        with self._r.pipeline() as pipe:
            pipe.multi()
            for b_id, betting_line in self.std_mngr.store_eids(league, betting_lines, BettingLines._get_key):
                pipe.hset(b_id, mapping=betting_line)
                pipe.sadd(f'{self.name}:idx:{betting_line["g_id"]}', b_id)
                self.subjects.setsubjactive(betting_line['s_id'])  # Set this subject as active so redis can reduce the number of box scores it stores
                self.live_mngr.track(league, b_id, betting_line['game_time'])
            pipe.execute()
