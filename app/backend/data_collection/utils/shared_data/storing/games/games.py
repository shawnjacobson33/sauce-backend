import threading
from typing import Optional
from collections import defaultdict


def get_game_info(away_team: dict, home_team: dict) -> str:
    return f"{away_team.get('full_name', away_team.get('abbr_name'))} @ {home_team.get('full_name', home_team.get('abbr_name'))}"


class Games:
    """
    _games: {
        ('NBA', '1239asd09' ( away team id )): {
            'info': 'BOS @ BKN',
            'box_score_url': 'NBA_20241113_BOS@BKN'
        },
        ('NBA', '9198asd' ( home team id )): {
            'info': 'BOS @ BKN',
            'box_score_url': 'NBA_20241113_BOS@BKN'
        },
        ...
    }

    """
    _games: defaultdict[tuple[str, str], dict] = defaultdict(dict)  # Gets the data stored in the database
    _lock1: threading.Lock = threading.Lock()

    @classmethod
    def get_games(cls) -> dict:
        # gets the data for the inputted partition
        return cls._games

    @classmethod
    def get_game(cls, league: str, team_id: str) -> Optional[dict]:
        # returns a game associated with the team id passed
        return cls._games.get((league, team_id))

    @classmethod
    def update_games(cls, game: dict) -> None:
        with cls._lock1:
            away_team, home_team = game['away_team'], game['home_team']
            away_team_key, home_team_key = (game['league'], away_team['abbr_name']), (game['league'], home_team['abbr_name'])
            cls._games[away_team_key] = cls._games[home_team_key] = {
                'game_time': game['game_time'],
                'info': get_game_info(away_team, home_team),
                'box_score_url': game['box_score_url']
            }

    @classmethod
    def size(cls, league: str = None) -> int:
        return int(len(cls._games) / 2)