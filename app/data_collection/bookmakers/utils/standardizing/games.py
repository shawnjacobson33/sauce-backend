from typing import Optional

from app.data_collection.utils.shared_data.games import Games


def get_game_id(team: Optional[dict]) -> Optional[dict[str, str]]:
    # get the games associated with the desired league
    if team and (stored_games := Games.get_games(team['league'])):
        # return the game data associated with a particular team, if it exists
        return stored_games.get(team.get('id'))


