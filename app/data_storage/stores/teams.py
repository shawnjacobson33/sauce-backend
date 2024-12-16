import redis

from app.data_storage.models import Team
from app.data_storage.stores.base import L2StaticDataStore


class Teams(L2StaticDataStore):
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

    def store(self, domain: str, teams: list[Team]) -> None:
        """
        Stores a list of teams into the Redis data store, associating each team with its domain.

        The method uses Redis pipelines for efficient batch operations, ensuring that each team's
        abbreviation and full name are stored with a unique identifier.

        Args:
            domain (str): The domain (partition) to associate the teams with.
            teams (list[Team]): A list of `Team` objects to store.

        Raises:
            AttributeError: If any expected attribute is missing or incorrectly set.
        """
        try:
            with self.__r.pipeline() as pipe:
                pipe.multi()
                for t_id, team in self._get_eids(domain, teams):
                    pipe.hsetnx(t_id, 'abbr', team.std_name)
                    pipe.hsetnx(t_id, 'full', team.full_name)

                pipe.execute()

        except AttributeError as e:
            self._attr_error_cleanup(e)