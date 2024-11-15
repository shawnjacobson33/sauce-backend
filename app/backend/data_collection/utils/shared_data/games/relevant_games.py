import threading
from collections import defaultdict
from typing import Optional

from app.backend.data_collection.utils.shared_data.games.all_games import AllGames


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
    _relevant_games: defaultdict[str, set] = defaultdict(set)
    _lock1: threading.Lock = threading.Lock()

    @classmethod
    def get_relevant_games(cls, league: Optional[str] = None):
        # gets the data for the inputted partition
        return cls._relevant_games.get(league) if league else cls._relevant_games

    @classmethod
    def update_games(cls, game: dict):
        # filter the games data structure
        stored_game = AllGames.get_game(game['league'], game['away_team']['id'])
        cls._relevant_games[game['league']].add(stored_game)

