from typing import Optional

from app.backend.data_collection import utils as dc_utils
from app.backend.data_collection.games.utils import GameSource


class BoxScoreRetriever(dc_utils.Retriever):
    def __init__(self, source: GameSource):
        super().__init__(source)

    def get_games_to_retrieve(self) -> Optional[dict]:
        # get games that are actively going on
        if active_games := dc_utils.ActiveGames.get_active_games(self.source.league):
            # get games that have players in them that have prop lines with bookmakers
            if relevant_games := dc_utils.RelevantGames.get_relevant_games(self.source.league):
                # only want games that are going on that are relevant
                return active_games.intersection(relevant_games)

        return active_games # TODO: change later

    def update_box_scores(self, game_id: str, subject: dict, box_score: dict, stat_type: str) -> None:
        # add the game to the shared data structure
        dc_utils.BoxScores.update_box_scores(self.source.league, game_id, subject, box_score, stat_type)
        # keep track of the number of games found per league
        self.data_collected += 1

    def __str__(self):
        return f'{str(self.data_collected)} ({self.source.league_specific}) games'