import threading
from collections import defaultdict
from typing import Optional


class ActiveGames:
    _active_games: defaultdict[str, dict] = dict()
    _lock1: threading.Lock = threading.Lock()

    @classmethod
    def get_active_games(cls, league: Optional[str]):
        # gets the data for the inputted partition
        return cls._active_games.get(league) if league else cls._active_games

    @classmethod
    def update_active_games(cls, game: dict):