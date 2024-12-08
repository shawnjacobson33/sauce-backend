from typing import Optional, Union

import redis

from app.data_storage.redis.structures import utils


class Teams:
    def __init__(self, r: redis.Redis):
        self.__r = r

    def _get_team_id(self, league: str, team: str) -> Optional[str]:
        return self.__r.hget(f'teams:lookup:{league}', key=team)

    def get(self, league: str, team: str, key: str = None) -> Optional[Union[dict[str, str], str]]:
        if team_id := self._get_team_id(league, team):
            return self.__r.hgetall(team_id) if not key else self.__r.hget(team_id, key=key)

        self._set_unidentified(league, team)

    def get_unidentified(self) -> Optional[set[str]]:
        return self.__r.smembers('teams:noid')

    def _set_unidentified(self, *args) -> None:
        self.__r.sadd('teams:noid', 'teams:{}:{}'.format(*args))

    def _set_team_id(self, team_id: str, league: str, teams: list[str]) -> str:
        t_id = utils.get_short_id(self.__r, 'teams')
        self.__r.hset(f'teams:lookup:{league}', key=team_id, value=t_id)
        for team in teams:
            self.__r.hsetnx(f'teams:std:{league}', key=team, value=team_id)

        return t_id

    def store(self, league: str, team: str, std_team: str, full_team: str) -> None:
        t_id = self._set_team_id(f'teams:{league}:{std_team}', league, teams=[team, std_team])
        self.__r.hset(t_id, mapping={
            'abbr_name': std_team,
            'full_name': full_team
        })