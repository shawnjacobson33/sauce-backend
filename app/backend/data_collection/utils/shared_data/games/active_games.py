import threading
from collections import defaultdict
from typing import Optional

from app.backend.data_collection.utils.shared_data.games.all_games import AllGames


class ActiveGames:
    """
    _active_games: {
        'NBA': {
            '123kxd90' ( game id ): {
                'info': 'BOS @ BKN',
                'box_score_url': 'NBA_20241113_BOS@BKN'
                ...
            }
            ...
        }
        ...
    }
    """
    _active_games: defaultdict[str, set] = defaultdict(set)
    _lock1: threading.Lock = threading.Lock()

    @classmethod
    def get_active_games(cls, league: Optional[str]):
        # gets the data for the inputted partition
        return cls._active_games.get(league) if league else cls._active_games

    @classmethod
    def update_games(cls, games: list[dict]):
        for game in games:
            # get game info stored in a structured way
            stored_game = AllGames.get_game(game['league'], game['away_team']['id'])
            # add the game to the set under its league
            cls._active_games[game['league']].add(stored_game)