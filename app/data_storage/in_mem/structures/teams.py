from collections import namedtuple
from typing import Optional, Union, Iterable

import redis

from app.data_storage.in_mem.structures import utils


NAMESPACE = {
    'hstd': 'teams:std:{}',
    'snoid': 'teams:noid',
}


class Teams:
    """
    A class for managing team data in a Redis database. The `Teams` class provides
    methods to store, retrieve, and manage team-related information, with features
    for handling unique identifiers, standardization, and tracking unmapped teams.

    Attributes:
        __r (redis.Redis): Redis client instance for database operations.
        __aid (utils.AutoId): Utility for generating auto-incrementing unique IDs.
        _hstd (str): Namespace for standard team data in Redis.
        _snoid (str): Namespace for unmapped teams in Redis.
        names (dict): Namespace dictionary for easy access to Redis keys.
    """
    def __init__(self, r: redis.Redis):
        """
        Initialize the Teams class.

        Args:
            r (redis.Redis): Redis client instance for managing team data.
        """
        self.__r = r
        self.__aid = utils.AutoId(r, 'teams')
        self._hstd = NAMESPACE['hstd']
        self._snoid = NAMESPACE['snoid']
        self.names = NAMESPACE

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

    def getnoids(self) -> Optional[set[str]]:
        """
        Retrieve a set of unmapped teams.

        Returns:
            Optional[set[str]]: A set of unmapped team identifiers.
        """
        return self.__r.smembers(self._snoid)

    def _set_noid(self, league: str, team: str) -> None:
        """
        Mark a team as unmapped in Redis.

        Args:
            league (str): The league to which the team belongs.
            team (str): The name of the team.
        """
        self.__r.sadd(self._snoid, f'{league}:{team}')

    def _check_for_std_key_existence(self, team: namedtuple) -> tuple[Optional[int], Optional[str]]:
        """
        Check if a standardized team key exists in Redis and update lookup structure.

        Args:
            team (namedtuple): A named tuple containing team attributes.

        Returns:
            Optional[str]: The team ID if it exists; otherwise, None.
        """
        f_hstd_name = self._hstd.format(team.league)
        if t_id := self.__r.hget(f_hstd_name, team.std_name):
            updated = self.__r.hsetnx(f_hstd_name, key=team.name, value=t_id)
            return updated, t_id

        return None, None

    def _update_std_key_lookup_struct(self, team: namedtuple) -> tuple[int, str]:
        """
        Update the lookup structure for standardized keys.

        Args:
            team (namedtuple): A named tuple containing team attributes.

        Returns:
            str: The new team ID.
        """
        new_updates = 0
        new_t_id = self.__aid.generate()
        f_hstd_name = self._hstd.format(team.league)
        for team_name in {team.name, team.std_name}:
            new_updates += self.__r.hsetnx(f_hstd_name, key=team_name, value=new_t_id)

        if not new_updates:
            self.__aid.decrement()

        return new_updates, new_t_id

    def _set_team_id(self, team: namedtuple) -> tuple[int, str]:
        """
        Set the ID for a team, creating or updating the necessary structures.

        Args:
            team (namedtuple): A named tuple containing team attributes.

        Returns:
            tuple[int, str]: number of value updates and The team ID.
        """
        updated, t_id = self._check_for_std_key_existence(team)
        return (updated, t_id) if t_id else self._update_std_key_lookup_struct(team)

    def store(self, teams: set[namedtuple]) -> None:
        """
        Store multiple teams in the Redis database.

        Args:
            teams (set[namedtuple]): A set of named tuples representing teams.
        """
        try:
            new_teams_stored = 0
            for team in teams:
                updates, t_id = self._set_team_id(team)
                if t_id:
                    if (self.__r.hsetnx(t_id, 'abbr', team.std_name) and
                            self.__r.hsetnx(t_id, 'full', team.full_name)):

                        new_teams_stored += 1
                        print(f"[Teams]: Successfully stored '{team.league}:{team.std_name}'!")

                    print(f"""[Teams]: '{team.league}:{team.std_name}' {" was updated with " + team.name if updates else " already exists!"}""")

            print(f"[Teams]: Stored {new_teams_stored} new teams!")

        except AttributeError as e:
            print("Error:", e)
            self.__aid.decrement()