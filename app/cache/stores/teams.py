import json
from collections import defaultdict
from typing import Optional, Iterable

import redis

from app.data_storage.models import Team
from app.data_storage.stores.base import DataStore


class Teams(DataStore):
    """
    A class that manages the storage and retrieval of team data in a Redis-backed data store.

    This class extends `L2DataStore` to handle operations specific to teams, such as storing team
    information (like abbreviations and full names) and retrieving team data by domain.
    """
    def __init__(self, r: redis.Redis):
        """
        Initializes the Teams instance.

        Args:
            r (redis.Redis): A Redis connection instance.
        """
        super().__init__(r, 'teams')

    def getid(self, league: str, team: str) -> Optional[str]:
        return self._r.hget(f'{self.lookup_name}:{league.lower()}', team)

    def _scan_team_ids(self, league: str) -> Iterable:
        counter = defaultdict(int)
        for _, team_id in self._r.hscan_iter(f'{self.lookup_name}:{league.lower()}'):
            if not counter[team_id]:
                counter[team_id] += 1
                yield team_id

    def getids(self, league: str) -> Iterable:
        yield from self._scan_team_ids(league)

    def getteam(self, league: str, team: str, report: bool = False) -> Optional[str]:
        if team_id := self.getid(league, team):
            team = self._r.hget(f'{self.info_name}:{league.lower()}', team_id)
            return json.loads(team)

    def getteams(self, league: str) -> Iterable:
        if team_ids := self.getids(league):
            for team_id in team_ids:
                yield self._r.hget(f'{self.info_name}:{league.lower()}', team_id)

    def _store_in_lookup(self, league: str, team: Team) -> Optional[str]:
        try:
            inserts = 0
            team_id = self.id_mngr.generate()
            for team_name in {team.name, team.std_name}:
                inserts += self._r.hsetnx(f'{self.lookup_name}:{league.lower()}', team_name, team_id)

            return team_id if inserts else self.id_mngr.decr()

        except IndexError as e:
            self._handle_error(e)

    def storeteams(self, league: str, teams: list[Team]) -> None:
        try:
            with self._r.pipeline() as pipe:
                pipe.multi()
                for team in teams:
                    if team_id := self._store_in_lookup(league, team):
                        pipe.hset(f'{self.info_name}:{league.lower()}', team_id, json.dumps({
                            'abbr': team.std_name,
                            'full': team.full_name
                        }))
                pipe.execute()

        except AttributeError as e:
            self._handle_error(e)