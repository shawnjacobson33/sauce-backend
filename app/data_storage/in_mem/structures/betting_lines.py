from typing import Optional, Union, Any

import redis

from app.data_storage.in_mem.structures import utils


class BettingLines:
    def __init__(self, r: redis.Redis):
        self.__r = r

    def _get_game_id(self, league: str, team: str) -> Optional[str]:
        team_id = f'{league}:{team}'
        return self.__r.hget(f'games:lookup:{league}', key=team_id)

    def get(self, league: str, team: str, key: str = None) -> Optional[Union[dict[str, str], str]]:
        if game_id := self._get_game_id(league, team):
            return self.__r.hgetall(game_id) if not key else self.__r.hget(game_id, key=key)

    def _get_live_ids(self) -> Optional[set[str]]:
        """Get any games, betting lines that are currently in-play and check to see if are just starting"""
        bl_ids = utils.get_live_ids(self.__r, 'lines:watch:game_time')
        utils.update_live_hash(self.__r, 'lines:index:live', bl_ids)
        return bl_ids

    def get_live(self) -> Optional[set[Union[str, dict[str, Any]]]]:
        if bl_ids := self._get_live_ids():
            return {self.__r.hgetall(bl_id) for bl_id in bl_ids}  # TODO: Consider using a generator for the data = True case...it could be a lot of data

    def _set_betting_line_id(self, betting_line_id: str) -> str:
        """Assigns this betting line id to a unique id, use less space, and then update the unique id"""
        b_id = utils.get_short_id(self.__r, 'lines')
        self.__r.hset(f'lines:lookup', key=betting_line_id, value=b_id)
        return b_id

    def _set_expirations(self, betting_line_id: str, bl_id: str, game_time: int) -> None:
        """Delete betting lines at game time"""
        self.__r.hexpireat(f'lines:lookup', game_time, betting_line_id)
        self.__r.hexpireat(f'lines:{bl_id}', game_time)

    def store(self, league: str, betting_line: dict[str, Any]) -> None:
        s_id = betting_line['subject_id']
        betting_line_id = f"""
            lines:{league}:{betting_line['league']}:
            {betting_line['market']}:{s_id}
        """

        bl_id = self._set_betting_line_id(betting_line_id)
        self.__r.hset(f"lines:{bl_id}", mapping=betting_line)
        self.__r.sadd(f"lines:index:{s_id}", bl_id)  # All betting lines for a particular subject -- used for box scores
        self.__r.sadd(f'subjects:active', s_id)  # Set this subject as active so redis can reduce the number of box scores it stores
        utils.watch_game_time(self.__r, 'lines:watch:game_time', bl_id, betting_line['game_time'])
