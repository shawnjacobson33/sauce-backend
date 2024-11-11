import threading
from collections import defaultdict


class Games:
    _games: defaultdict = defaultdict(list)
    _lock1: threading.Lock = threading.Lock()

    @classmethod
    def get_games(cls):
        return cls._games

    @classmethod
    def update_games(cls, league: str, game: dict):
        with cls._lock1:
            cls._games[league].append(game)

