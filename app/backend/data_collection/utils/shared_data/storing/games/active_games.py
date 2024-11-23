import threading
from collections import defaultdict
from typing import Optional


class ActiveGames:
    """
    _active_games: {
        'NBA': {
            '123kxd90' ( game id ): {
                'id': '123kxd90',
                'info': 'BOS @ BKN',
                'box_score_url': 'NBA_20241113_BOS@BKN'
                ...
            }
            ...
        }
        ...
    }
    """
    _active_games: defaultdict[str, dict] = defaultdict(lambda: defaultdict(dict))
    _lock1: threading.Lock = threading.Lock()

    @classmethod
    def get_active_games(cls, league: Optional[str]):
        # gets the data for the inputted partition
        return cls._active_games.get(league) if league else cls._active_games

    @classmethod
    def update_active_games(cls, games: list[dict]):
        # for each active game
        for game in games:
            # restructure the data dictionary
            game['id'] = str(game['_id'])
            away_team, home_team = game.pop('away_team'), game.pop('home_team')
            game[away_team['abbr_name']], game[home_team['abbr_name']] = away_team, home_team
            # add the game to the set under its league
            cls._active_games[game['league']][game['id']] = {
                key: value for key, value in game.items() if key not in {'time_processed', 'game_time', 'league',
                                                                         'source'}
            }