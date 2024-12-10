import threading
from collections import defaultdict
from typing import Any


class ActiveGames:
    """
    _active_games: {
        '('NBA', 'BOS @ ATL'): 'NBA_20241113_BOS@BKN'
        ...
    }
    """
    _active_games: defaultdict[tuple[str, str], dict[str, Any]] = defaultdict(dict)
    _lock1: threading.Lock = threading.Lock()

    @classmethod
    def get_active_games(cls, league: str = None) -> dict:
        return {game_id: url for game_id, url in cls._active_games.items() if game_id[0] == league} if league else cls._active_games

    @classmethod
    def update_active_games(cls, games: list[dict]):
        # for each active game
        for game in games:
            cls._active_games[(game['league'], game['info'])] = game['box_score_url']