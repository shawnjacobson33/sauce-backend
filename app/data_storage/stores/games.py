from datetime import datetime
from typing import Optional, Iterable

import json
import redis

from app.data_storage.models import Game
from app.data_storage.stores.base import DynamicDataStore


class Games(DynamicDataStore):
    def __init__(self, r: redis.Redis):
        """
        Initializes the Games class with a Redis client instance.

        Args:
            r (redis.Redis): The Redis client used to interact with the Redis database.
        """
        super().__init__(r, 'games')

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

    def getgames(self, league: str = None, game_ids: list[str] = None) -> Iterable:
        if game_ids:
            for game_id in game_ids: yield self._r.hget(f'{self.name}:{league.lower()}', game_id)
        elif league:
            yield from self.get_entities('secondary', league)
        else:
            raise ValueError("Either league or game IDs must be provided.")

    def _scan_for_live_games(self, league: str, threshold: int = 30) -> Iterable:
        for g_id in self._r.hscan_iter(f'{self.name}:{league.lower()}'):
            if game_json := self._r.hget(f'{self.name}:{league.lower()}', g_id):
                game_dict = json.loads(game_json)
                if (game_dict['game_time'] - int(datetime.now().timestamp())) < threshold:
                    self._r.hset(f'{self.name}:live:{league}', g_id, game_json)
                    self._r.hdel(f'{self.name}:{league.lower()}', g_id)
                    yield game_dict

    def getlivegames(self, league: str) -> Iterable:
        for game in self._scan_for_live_games(league, threshold=30): yield game

    def getcompletedgames(self, league: str) -> Iterable:
        for game in self._r.sscan_iter(f'games:completed:{league}'): yield game

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

    @staticmethod
    def _get_expiration(game: Game) -> int:
        return int(game.game_time.timestamp())

    def storegames(self, league: str, games: list[Game]) -> None:
        try:
            with self._r.pipeline() as pipe:
                pipe.multi()
                for game_id, game in self.lookup_mngr.store_entity_ids(league, games, self._get_keys, self._get_expiration):
                    pipe.hset(f'{self.name}:{league.lower()}', game_id, json.dumps({
                        'info': game.info,
                        'game_time': game.game_time.strftime("%Y-%m-%d %H:%M")
                    }))
                pipe.execute()

        except KeyError as e:
            self._handle_error(e)
