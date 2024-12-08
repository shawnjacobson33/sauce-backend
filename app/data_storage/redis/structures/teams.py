from typing import Optional, Union

import redis

from app.data_storage.redis.structures import utils


class Teams:
    def __init__(self, r: redis.Redis):
        self.__r = r

    def _get_team_id(self, league: str, team: str) -> Optional[str]:
        return self.__r.hget(f'teams:std:{league}', key=team)

    def get(self, league: str, team: str, key: str = None) -> Optional[Union[dict[str, str], str]]:
        if team_id := self._get_team_id(league, team):
            return self.__r.hgetall(team_id) if not key else self.__r.hget(team_id, key=key)

        self._set_unidentified(league, team)

    def get_unidentified(self) -> Optional[set[str]]:
        return self.__r.smembers('teams:noid')

    def _set_unidentified(self, league: str, team: str) -> None:
        self.__r.sadd('teams:noid', f'{league}:{team}')

    def _set_team_id(self, league: str, teams: list[str]) -> Optional[str]:
        if not self.__r.hget(f'teams:std:{league}', teams[0]):
            t_id = utils.get_auto_id(self.__r, 'teams')
            with self.__r.pipeline() as pipe:
                pipe.multi()
                for team in teams:
                    pipe.hsetnx(f'teams:std:{league}', key=team, value=t_id)

                pipe.execute()

            return t_id

        print(f'Team: {teams[0]} already stored!')

    def store(self, league: str, team: str, std_team: str, full_team: str) -> None:
        if t_id := self._set_team_id(league, teams=[team, std_team]):
            self.__r.hset(t_id, mapping={
                'abbr_name': std_team,
                'full_name': full_team
            })