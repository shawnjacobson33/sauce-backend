from typing import Optional, Iterable

import redis

from app.data_storage.models import Game
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

    def getid(self, league: str, team: str) -> Optional[str]:
        """
        Retrieves the unique identifier (ID) for a team.

        Args:
            league (str): The name of the league to filter by.
            team (str): The name of the team to retrieve the ID for.

        Returns:
            Optional[str]: The ID of the team if it exists, otherwise None.
        """
        return self.std_mngr.get_eid(league, team)

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

    def getliveids(self, league: str) -> Iterable:
        """
        Retrieves all live games for a given league.

        Args:
            league (str): The name of the league to filter by.

        Yields:
            Iterable: A generator yielding live game entities based on the provided criteria.
        """
        yield from self.live_mngr.get_and_store_live_ids(league)

    def getcompletedgameids(self) -> Iterable:
        # if a games is over then, start some process to label all betting lines using their corresponding
        # box scores. Then remove the game from the live game store. Default expire after 6 hours.
        yield from self._r.sscan_iter(f'{self.name}:completed')

    def getgame(self, league: str, team: str, report: bool = False) -> Optional[str]:
        """
        Retrieves a specific game for a given team.

        Args:
            league (str): The name of the league to filter by.
            team (str): The name of the team to filter by.
            report (bool, optional): Whether to include a report in the result. Defaults to False.

        Returns:
            Optional[str]: The game entity ID, or None if the game could not be found.
        """
        return self.get_entity('secondary', league, team, report=report)

    def getgames(self, league: str = None, g_ids: list[str] = None) -> Iterable:
        """
        Retrieves all games for a given league, optionally filtering by live status.

        Args:
            league (str): The name of the league to filter by.
            g_ids (list[str]): A list of game IDs to retrieve.
        Yields:
            Iterable: A generator yielding game entities based on the provided criteria.
        """
        if g_ids:
            for g_id in g_ids: yield self._r.hgetall(g_id)
        elif league:
            yield from self.get_entities('secondary', league)
        else:
            raise ValueError("Either league or game IDs must be provided.")

    def setlive(self, league: str, g_ids: list[str]) -> None:
        # transfer the game to a live game store if the game is active
        # have a default expire time of 6 hours
        live_games = self.getgames(league, g_ids)
        self.live_mngr.store(league, g_ids, live_games)

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

    def storegames(self, league: str, games: list[Game]) -> None:
        """
        Stores game data in Redis, optionally converting game times to timestamps.

        Args:
            league (str): The league to which the games belong.
            games (list[Game]): A list of Game objects to store.

        Raises:
            KeyError: If a KeyError occurs during the storage process.
        """
        try:
            with self._r.pipeline() as pipe:
                pipe.multi()
                for g_id, game in self.std_mngr.store_eids(league, games, Games._get_keys):
                    pipe.hset(g_id, mapping={
                        'info': game.info,
                        'game_time': game.game_time.strftime("%Y-%m-%d %H:%M")
                    })
                    pipe.hexpireat(g_id, game.game_time)
                pipe.execute()

        except KeyError as e:
            self._handle_error(e)
