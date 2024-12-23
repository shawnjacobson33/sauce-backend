from typing import Optional, Iterable

import redis

from app.data_storage.models import Team
from app.data_storage.stores.base import StaticDataStore


class Teams(StaticDataStore):
    """
    A class that manages the storage and retrieval of team data in a Redis-backed data store.

    This class extends `L2StaticDataStore` to handle operations specific to teams, such as storing team
    information (like abbreviations and full names) and retrieving team data by domain.
    """
    def __init__(self, r: redis.Redis):
        """
        Initializes the Teams instance.

        Args:
            r (redis.Redis): A Redis connection instance.
        """
        super().__init__(r, 'teams')

    def getid(self, team: Team) -> Optional[str]:
        """
        Retrieve the unique identifier for a specific team within a league.

        Args:
            team (Team): A team object.

        Returns:
            Optional[str]: The unique identifier for the team, or None if not found.
        """
        return self.std_mngr.get_eid(team.domain, team.name)

    def getids(self, league: str = None) -> Iterable:
        """
        Retrieve all unique identifiers for teams, optionally filtered by league.

        Args:
            league (str, optional): The league to filter by. If None, retrieves IDs across all leagues.

        Yields:
            str: Team identifiers.
        """
        yield from self.std_mngr.get_eids(league)

    def getteam(self, league: str, team: str, report: bool = False) -> Optional[str]:
        """
        Retrieve details about a specific team within a league.

        Args:
            league (str): The league or domain to search within.
            team (str): a team name
            report (bool, optional): Whether to log or report missing entries. Defaults to False.

        Returns:
            Optional[str]: Details of the team, or None if not found.
        """
        return self.get_entity('secondary', league, team, report=report)

    def getteams(self, league: str) -> Iterable:
        """
        Retrieve all teams within a specific league.

        Args:
            league (str): The name of the league.

        Yields:
            str: Team identifiers within the specified league.
        """
        yield from self.get_entities('secondary', league)

    def store(self, league: str, teams: list[Team]) -> None:
        """
        Stores a list of teams into the Redis data store, associating each team with its domain.

        The method uses Redis pipelines for efficient batch operations, ensuring that each team's
        abbreviation and full name are stored with a unique identifier.

        Args:
            league (str): The domain (partition) to associate the teams with.
            teams (list[Team]): A list of `Team` objects to store.

        Raises:
            AttributeError: If any expected attribute is missing or incorrectly set.
        """
        try:
            with self._r.pipeline() as pipe:
                pipe.multi()
                for t_id, team in self.std_mngr.store_eids(league, teams, keys=lambda tm: (tm.name, tm.std_name)):
                    pipe.hset(t_id, mapping={
                        'abbr': team.std_name,
                        'full': team.full_name
                    })

                pipe.execute()

        except AttributeError as e:
            self._handle_error(e)