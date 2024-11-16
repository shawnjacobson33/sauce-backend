import threading
from collections import defaultdict
from typing import Optional


class RelevantGames:
    """
    _relevant_games: {
        'NBA': (
            {
                'id': '123kxd90'
                'info': 'BOS @ BKN',
                'box_score_url': 'NBA_20241113_BOS@BKN'
                ...
            }
            ...
        )
    }
    """
    _relevant_games: defaultdict[str, dict] = defaultdict(dict)
    _lock1: threading.Lock = threading.Lock()

    @classmethod
    def get_relevant_games(cls, league: Optional[str] = None):
        # gets the data for the inputted partition
        return cls._relevant_games.get(league) if league else cls._relevant_games

    @classmethod
    def update_games(cls, game: dict, league: str) -> None:
        cls._relevant_games[league][game['id']] = {key: value for key, value in game.items() if key != 'id'}

