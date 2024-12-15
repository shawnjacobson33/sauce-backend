from collections import namedtuple
from typing import Optional, Union, Iterable

import redis

from app.data_storage.in_mem.structures.utils import StaticDataStore


class Teams(StaticDataStore):
    """
    A class to manage team data using Redis as the backend storage.

    Inherits from StaticDataStore, providing capabilities to store and retrieve
    static team data efficiently. The `Teams` class operates on the 'teams' key
    namespace in the Redis datastore.
    """
    def __init__(self, r: redis.Redis):
        """
        Initialize the Teams instance with a Redis client.

        Args:
            r (redis.Redis): An instance of the Redis client for accessing the Redis database.
        """
        super().__init__(r, 'teams')

    def getteamid(self, league: str, team: str) -> Optional[str]:
        """
        Get the ID of a team in a specific league.

        Args:
            league (str): The league to which the team belongs.
            team (str): The name of the team.

        Returns:
            Optional[str]: The ID of the team, if available.
        """
        f_hstd_name = self._hstd.format(league)
        return self.__r.hget(f_hstd_name, key=team)

    def getteam(self, league: str, team: str, key: str = None, report: bool = False) -> Optional[Union[dict[str, str], str]]:
        """
        Retrieve a team's details or a specific attribute.

        Args:
            league (str): The league to which the team belongs.
            team (str): The name of the team.
            key (str, optional): Specific attribute to retrieve. Defaults to None.
            report (bool, optional): Whether to report the team as unmapped if not found. Defaults to False.

        Returns:
            Optional[Union[dict[str, str], str]]: Team details as a dictionary or a specific attribute value.
        """
        if team_id := self.getteamid(league, team):
            return self.__r.hgetall(team_id) if not key else self.__r.hget(team_id, key=key)

        if report:
            self._set_noid(league, team)

    def getteamids(self, league: str = None) -> Iterable:
        """
        Retrieve team identifiers for all teams or for teams in a specific league.

        Args:
            league (str, optional): The league to filter teams by. If not specified,
                                    returns identifiers for all teams. Defaults to None.

        Returns:
            Iterable: A generator that yields team identifiers or team details as dictionaries.
        """
        if not league:
            return (t_key for t_key in self.__r.scan_iter('team:*'))

        return (self.__r.hget(self._hstd, t_key) for t_key in self.__r.hscan_iter(self._hstd.format(league)))

    def getteams(self, league: str = None) -> Iterable:
        """
        Retrieve details of all teams or teams in a specific league.

        Args:
            league (str, optional): The league to filter teams by. If not specified,
                                    returns details for all teams. Defaults to None.

        Returns:
            Iterable: A generator that yields dictionaries containing team details.
        """
        if not league:
            return (self.__r.hgetall(t_key) for t_key in self.__r.scan_iter('team:*'))

        return (self.__r.hgetall(t_id) for t_id in self.__r.hscan_iter(self._hstd.format(league)))

    def _set_noid(self, league: str, team: str) -> None:
        """
        Mark a team as unmapped in Redis.

        Args:
            league (str): The league to which the team belongs.
            team (str): The name of the team.
        """
        self.__r.sadd(self._snoid, f'{league}:{team}')

    def store(self, teams: set[namedtuple]) -> None:
        """
        Store multiple teams in the Redis database.

        Args:
            teams (set[namedtuple]): A set of named tuples representing teams.
        """
        try:
            for team in teams:
                self.partition = team.league
                if t_id := self._eval_entity(team):
                    self.__r.hsetnx(t_id, 'abbr', team.std_name)
                    self.__r.hsetnx(t_id, 'full', team.full_name)

        except AttributeError as e:
            print(f"[Teams]: ERROR --> {e}")
            self._hstd_manager.aid.decrement()  # TODO: Fix this