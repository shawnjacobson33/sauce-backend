from typing import Optional, Union, Any
from datetime import datetime

import redis

from app.data_storage.redis.structures import utils


class Games:
    def __init__(self, r: redis.Redis):
        self.__r = r

    def _get_game_id(self, league: str, team: str) -> Optional[str]:
        team_id = f'{league}:{team}'
        return self.__r.hget(f'games:lookup:{league}', key=team_id)

    def get(self, league: str, team: str, key: str = None) -> Optional[Union[dict[str, str], str]]:
        if game_id := self._get_game_id(league, team):
            return self.__r.hgetall(game_id) if not key else self.__r.hget(game_id, key=key)

    def _get_live_ids(self, league: str) -> Optional[set[str]]:
        """Get any games, betting lines that are currently in-play and check to see if are just starting"""
        g_ids = utils.get_live_ids(self.__r, f'games:watch:game_time:{league}')
        utils.update_live_hash(self.__r, f'games:index:live:{league}', g_ids)
        return g_ids

    def get_live(self, league: str) -> Optional[set[Union[str, dict[str, Any]]]]:
        """Get game ids currently in play"""
        if g_ids := self._get_live_ids(league):
            return {self.__r.hgetall(g_id) for g_id in g_ids}  # TODO: Consider using a generator for the data = True case...it could be a lot of data

    def _set_game_id(self, game_id: str, league: str, teams: list[str]) -> None:
        lookup_id = f'games:lookup:{league}'
        for team in teams:
            self.__r.hsetnx(lookup_id, key=f'{league}:{team}', value=game_id)

    def store(self, league: str, box_score_url: str, game_time: datetime, game_info: str) -> None:
        game_id = f'games:{box_score_url}'
        timestamp = utils.convert_to_timestamp(game_time)
        self._set_game_id(game_id, league, teams=game_info.split(' @ '))
        self.__r.hset(game_id, mapping={
            'game_time': timestamp,
            'info': game_info
        })
        utils.watch_game_time(self.__r, f'games:watch:game_time:{league}', key=game_id, game_time=timestamp)
