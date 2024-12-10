from collections import namedtuple
from typing import Optional, Union

import redis

from app.data_storage.redis.structures import utils


class Teams:
    def __init__(self, r: redis.Redis):
        self.__r = r
        self.__aid = utils.AutoId(r, 'teams')

    def _get_team_id(self, league: str, team: str) -> Optional[str]:
        return self.__r.hget(f'teams:std:{league}', key=team)

    def getteam(self, league: str, team: str, key: str = None, report: bool = False) -> Optional[Union[dict[str, str], str]]:
        if team_id := self._get_team_id(league, team):
            return self.__r.hgetall(team_id) if not key else self.__r.hget(team_id, key=key)

        if report:
            self._set_unidentified(league, team)

    def getid(self, league: str, team: str) -> Optional[str]:
        return self._get_team_id(league, team)

    def getall(self, league: str = None) -> list:
        if not league:
            return [self.__r.hgetall(t_id) for t_id in self.__r.keys('team:*')]

        lt_ids = set(self.__r.hgetall(f'teams:std:{league}').values())
        return [self.__r.hgetall(lt_id) for lt_id in lt_ids]

    def getnoid(self) -> Optional[set[str]]:
        return self.__r.smembers('teams:noid')

    def _set_unidentified(self, league: str, team: str) -> None:
        self.__r.sadd('teams:noid', f'{league}:{team}')

    def _set_team_id(self, team: namedtuple) -> str:
        tms_std_name = f'teams:std:{team.league}'
        if t_id := self.__r.hget(tms_std_name, team.std_name):
            self.__r.hsetnx(tms_std_name, key=team.name, value=t_id)
            return t_id

        new_updates = 0
        new_t_id = self.__aid.generate()
        for team_name in {team.name, team.std_name}:
            new_updates += self.__r.hsetnx(tms_std_name, key=team_name, value=new_t_id)

        if not new_updates:
            self.__aid.decrement()

        return new_t_id

    def store(self, teams: set[namedtuple]) -> None:
        try:
            new_teams_stored = 0
            for team in teams:
                if t_id := self._set_team_id(team):
                    if (self.__r.hsetnx(t_id, 'abbr', team.std_name) and
                            self.__r.hsetnx(t_id, 'full', team.full_name)):

                        new_teams_stored += 1
                        print(f"[Teams]: Successfully stored '{team.league}:{team.std_name}'!")

                    print(f"[Teams]: '{team.league}:{team.std_name}' already exists!")

            print(f"[Teams]: Stored {new_teams_stored} new teams!")

        except AttributeError as e:
            print("Error:", e)
            self.__aid.decrement()