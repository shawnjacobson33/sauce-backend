import threading
from collections import defaultdict
from typing import Optional


class RelevantGames:
    """
    _relevant_games: set (
        ('NBA', 'BOS @ ATL')
    )
    """
    _relevant_games: set[tuple[str, str]] = set()
    _lock: threading.Lock = threading.Lock()

    @classmethod
    def get_relevant_games(cls, league: str = None):
        return set([game_id for game_id in cls._relevant_games if game_id[0] == league]) if league else cls._relevant_games

    @classmethod
    def update_games(cls, game: dict) -> None:
        with cls._lock:
            cls._relevant_games.add((game['league'], game['info']))

