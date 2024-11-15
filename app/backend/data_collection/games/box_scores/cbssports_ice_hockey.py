import asyncio

from bs4 import BeautifulSoup

from app.backend.data_collection.games import utils as gm_utils
from app.backend.data_collection.games.box_scores import utils as bs_utils



class IceHockeyBoxScoreRetriever(bs_utils.BoxScoreRetriever):
    def __init__(self, source: gm_utils.GameSource):
        super().__init__(source)

    async def retrieve(self) -> None:
        # initialize a list of requests to make
        tasks = list()
        # for every game
        for game in self.get_games_to_retrieve():
            # Get the URL for the NBA schedule
            url = gm_utils.get_url(self.source.name, 'box_scores')
            # format the url with the unique url piece stored in the game dictionary
            formatted_url = url.format(game['box_score_url'])
            # Asynchronously request the data and call parse schedule for each formatted URL
            tasks.append(gm_utils.fetch(formatted_url, self._parse_box_score, game['id']))

        # gather all requests asynchronously
        await asyncio.gather(*tasks)

    async def _parse_box_score(self, html_content, game_id: str) -> None:
        # initializes a html parser
        soup = BeautifulSoup(html_content, 'html.parser')
        # extracts every starter and bench players box score table for both teams --  4 in total
        tables = soup.find_all('table', {'class': 'stats-table'})
        # for each table
        for table in tables: