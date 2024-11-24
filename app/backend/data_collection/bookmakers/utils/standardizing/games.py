from typing import Optional

from app.backend.data_collection.utils.shared_data.storing import Games, RelevantGames


def get_game_id(league: str, team_id: str) -> Optional[dict[str, str]]:
    # get the game associated with this team
    if matched_game := Games.get_game(league, team_id):
        # update relevant games data structure because it is used by a bookmaker
        RelevantGames.update_games(matched_game, league)
        # return the game data associated with a particular team, if it exists
        return matched_game
