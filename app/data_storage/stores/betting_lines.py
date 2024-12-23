from typing import Optional, Union, Any, Iterable

import redis

from app.data_storage.stores import utils, DynamicDataStore


class BettingLines(DynamicDataStore):
    def __init__(self, r: redis.Redis):
        super().__init__(r, 'betting_lines')

    def getbettinglines(self, league: str, is_live: bool = False) -> Iterable:
        yield from (self.get_live_entities(league) if is_live else self.get_entities('secondary', league))

    def store(self, league: str, betting_lines: list[dict]) -> None:
        s_id = betting_line['subject_id']
        betting_line_id = f"""
            lines:{league}:{betting_line['league']}:
            {betting_line['market']}:{s_id}
        """

        bl_id = self._set_betting_line_id(betting_line_id)
        self._r.hset(f"lines:{bl_id}", mapping=betting_line)
        self._r.sadd(f"lines:index:{s_id}", bl_id)  # All betting lines for a particular subject -- used for box scores
        self._r.sadd(f'subjects:active', s_id)  # Set this subject as active so redis can reduce the number of box scores it stores
        utils.watch_game_time(self._r, 'lines:watch:game_time', bl_id, betting_line['game_time'])
