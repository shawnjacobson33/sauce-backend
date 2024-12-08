from typing import Optional

from app import Games, RelevantGames


def get_game(team_id: tuple[str, str]) -> Optional[dict[str, str]]:
    # DATA LOOKS LIKE --> {'info': 'BOS @ BKN','box_score_url': 'NBA_20241113_BOS@BKN','game_time': 2024-12-03 20:00:00}
    if matched_game := Games.get_game(team_id):
        # update relevant games data structure because it is used by a bookmaker
        RelevantGames.update_games(matched_game)
        # return the game data associated with a particular team, if it exists
        return matched_game
