import asyncio
from typing import Optional

from backend.app.data_collection.workers import utils as dc_utils
from backend.app.data_collection.workers.logistics import utils as lg_utils
from backend.app.data_collection.workers.logistics.games.utils import GameSource


class BoxScoreRetriever(dc_utils.Retriever):
    def __init__(self, source: GameSource):
        super().__init__(source)
        self.league_spec = source.league_spec

    def get_games_to_retrieve(self) -> Optional[dict]:
        # get games that are actively going on
        if active_games := dc_utils.ActiveGames.get_active_games(self.league_spec):
            # get games that have players in them that have prop lines with bookmakers
            if relevant_games := dc_utils.RelevantGames.get_relevant_games(self.league_spec):
                # only want games that are going on that are relevant
                valid_game_ids = set(game_id for game_id in active_games).intersection(relevant_games)
                # return unique active games that are being used by bookmakers
                return {game_id: active_games[game_id] for game_id in valid_game_ids}

        return active_games # TODO: change later

    async def retrieve(self) -> None:
        # initialize a list of requests to make
        tasks = list()
        # get the games to retrieve box scores from if there are any
        if games_to_retrieve := self.get_games_to_retrieve():
            # for every game
            for game_id, box_score_url in games_to_retrieve.items():
                # Get the URL for the NBA schedule
                url_data = lg_utils.get_url(self.name, self.league_spec, 'box_scores')
                # format the url with the unique url piece stored in the game dictionary
                formatted_url = url_data['url'].format(url_data['league'], box_score_url)
                # Asynchronously request the data and call parse schedule for each formatted URL
                tasks.append(lg_utils.fetch(formatted_url, self._parse_box_score, game_id))

            # gather all requests asynchronously
            await asyncio.gather(*tasks)

    async def _parse_box_score(self, html_content: str, game_id: str) -> None:
        pass

    def update_box_scores(self, game_id: tuple[str, str], subject: dict, box_score: dict, stat_type: str) -> None:
        # add the game to the shared data structure
        dc_utils.BoxScores.update_box_scores(game_id, subject, box_score, stat_type)
        # keep track of the number of games found per league
        self.data_collected += 1

    def __str__(self):
        return f'{str(self.data_collected)} ({self.league_spec}) player stat lines collected'