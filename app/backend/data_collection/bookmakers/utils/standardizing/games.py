from typing import Optional

from app.backend.data_collection.utils.shared_data.games import AllGames, RelevantGames


def get_game_id(team: Optional[dict]) -> Optional[dict[str, str]]:
    # get the games associated with the desired league
    if team and (stored_games := AllGames.get_games(team['league'])):
        # get the game associated with this team
        matched_game = stored_games.get(team.get('id'))
        # update relevant games data structure because it is used by a bookmaker
        RelevantGames.update_games(matched_game)
        # return the game data associated with a particular team, if it exists
        return matched_game


