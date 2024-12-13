from typing import Optional, Union, Any
from datetime import datetime

import redis

from app.ds.in_mem.structures import Teams, utils


NAMESPACE = {
    'hstd': 'games:std:{}',
    'slive': 'games:live:{}',
    'zwatch': 'games:watch:{}'
}


class Games:
    def __init__(self, r: redis.Redis, teams: Teams):
        self.__r = r
        self.teams = teams
        self._hstd = NAMESPACE['hstd']
        self._slive = NAMESPACE['slive']
        self._zwatch = NAMESPACE['zwatch']
        self.__aid = utils.AutoId(r, 'games')

    def getgameid(self, league: str, t_id: str) -> Optional[str]:
        return self.__r.hget(self._hstd.format(league), key=t_id)

    def getgameinfo(self, league: str, t_id: str, key: str = None) -> Optional[Union[dict[str, str], str]]:
        if game_id := self.getgameid(league, t_id):
            return self.__r.hgetall(game_id) if not key else self.__r.hget(game_id, key=key)

    def getlivegameids(self, league: str) -> Optional[set[str]]:
        """Get any games, betting lines that are currently in-play and check to see if are just starting"""
        return utils.get_live_ids(self.__r, self._slive.format(league), self._zwatch.format(league))

    def getlivegames(self, league: str) -> Optional[set[Union[str, dict[str, Any]]]]:
        """Get game ids currently in play"""
        if g_ids := self.getlivegameids(league):
            return {self.__r.hgetall(g_id) for g_id in g_ids}  # TODO: Consider using a generator for the data = True case...it could be a lot of data

    def _set_game_id(self, game_id: str, league: str, teams: list[str]) -> None:
        f_hstd_name = self._hstd.format(league)
        for team in teams:
            self.__r.hsetnx(f_hstd_name, key=f'{league}:{team}', value=game_id)

    def store(self, league: str, team: str, game_time: datetime, mapping: dict) -> None:
        new_g_id = self.__aid.generate()
        timestamp = utils.convert_to_timestamp(game_time)
        self._set_game_id(game_id, league, teams=game_info.split(' @ '))
        self.__r.hset(game_id, mapping={
            'game_time': timestamp,
            'info': mapping['game_info']
        })
        utils.watch_game_time(self.__r, f'games:watch:game_time:{league}', key=game_id, game_time=timestamp)
