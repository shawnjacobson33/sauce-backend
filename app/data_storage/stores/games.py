from typing import Optional, Iterable

import redis

from app.data_storage.models import Team, Game
from app.data_storage.stores.utils import convert_to_timestamp
from app.data_storage.stores.base import DynamicDataStore


class Games(DynamicDataStore):
    """
    A class to manage game data storage and retrieval in a Redis-based system.

    Inherits from DynamicDataStore to leverage Redis-based data management
    capabilities for games, including retrieving, storing, and manipulating game data.
    """
    def __init__(self, r: redis.Redis):
        """
        Initializes the Games class with a Redis client instance.

        Args:
            r (redis.Redis): The Redis client used to interact with the Redis database.
        """
        super().__init__(r, 'games')

    def getid(self, team: Team) -> Optional[str]:
        """
        Retrieves the unique identifier (ID) for a team.

        Args:
            team (Team): The team object containing domain and name attributes.

        Returns:
            Optional[str]: The ID of the team if it exists, otherwise None.
        """
        return self.std_mngr.get_eid(team.domain, team.name)

    def getids(self, league: str = None, is_live: bool = False) -> Iterable:
        """
        Retrieves the game IDs for a league, optionally filtering by live status.

        Args:
            league (str, optional): The name of the league to filter by. Defaults to None.
            is_live (bool, optional): Whether to retrieve live game IDs. Defaults to False.

        Yields:
            Iterable: A generator yielding game IDs based on the provided criteria.
        """
        live_iter = self.live_mngr.getgameids(league)
        reg_iter = self.std_mngr.get_eids(league)
        yield from (live_iter if is_live else reg_iter)

    def getgame(self, team: Team, report: bool = False) -> Optional[str]:
        """
        Retrieves a specific game for a given team.

        Args:
            team (Team): The team object containing domain and name attributes.
            report (bool, optional): Whether to include a report in the result. Defaults to False.

        Returns:
            Optional[str]: The game entity ID, or None if the game could not be found.
        """
        return self.get_entity('secondary', team.domain, team.name, report=report)

    def getgames(self, league: str, is_live: bool = False) -> Iterable:
        """
        Retrieves all games for a given league, optionally filtering by live status.

        Args:
            league (str): The name of the league to filter by.
            is_live (bool, optional): Whether to retrieve live games. Defaults to False.

        Yields:
            Iterable: A generator yielding game entities based on the provided criteria.
        """
        yield from (self.get_live_entities(league) if is_live else self.get_entities(league))

    @staticmethod
    def _get_keys(game: Game) -> tuple[str, str]:
        """
        Extracts the away and home team names from a game entity's info field.

        Args:
            game (Game): The game entity containing the game information.

        Returns:
            tuple[str, str]: A tuple containing the away and home team names.
        Raises:
            IndexError: If an error occurs while extracting the team names from the game info.
        """
        try:
            away_team, home_team = game.info.split('_')[2].split('@')
            return away_team, home_team
        except IndexError as e:
            raise Exception(f"Error extracting team names from game info url (correct ex: NBA_20241113_BOS@BKN): {e}")

    def store(self, league: str, games: list[Game], to_timestamp: bool = False) -> None:
        """
        Stores game data in Redis, optionally converting game times to timestamps.

        Args:
            league (str): The league to which the games belong.
            games (list[Game]): A list of Game objects to store.
            to_timestamp (bool, optional): Whether to convert game times to timestamps. Defaults to False.

        Raises:
            KeyError: If a KeyError occurs during the storage process.
        """
        try:
            if to_timestamp:
                for game in games: game.game_time = convert_to_timestamp(game.game_time)

            for g_id, game in self.std_mngr.store_eids(league, games, Games._get_keys):
                self._r.hset(g_id, mapping={
                    'info': game.info,
                    'game_time': game.game_time
                })
                self.live_mngr.track_game(league, g_id, game.game_time)

        except KeyError as e:
            self._handle_error(e)

    # def flush(self, name: str, league: str) -> None:
    #     f_name = NAMESPACE[name].format(league)
    #     iter_keys = self._r.hscan_iter(f_name) if 'std' in f_name else self._r.sscan_iter(f_name)
    #     keys = list(iter_keys)
    #     self._r.delete(*keys)
    #     print(f"[Games]: FOR {f_name} Deleted {keys[0]}, {keys[1]}, ...")

    # def flushall(self) -> None:
    #     std_keys = list(self._r.hscan_iter('std'))
    #     slive_keys = list(self._r.sscan_iter('slive'))
    #     zwatch_keys = list(self._r.zscan_iter('zwatch'))
    #     with self._r.pipeline() as pipe:
    #         pipe.multi()
    #         self._r.delete(*std_keys)
    #         self._r.delete(*slive_keys)
    #         self._r.delete(*zwatch_keys)
    #         pipe.execute()