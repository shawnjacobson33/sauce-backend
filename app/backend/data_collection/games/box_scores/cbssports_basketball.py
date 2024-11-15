import asyncio

from bs4 import BeautifulSoup

from app.backend.data_collection.games import utils as gm_utils
from app.backend.data_collection.games.box_scores import utils as bs_utils


class BasketballBoxScoreRetriever(bs_utils.BoxScoreRetriever):
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
            # gets all rows in the box score table for starters or bench
            if (rows := table.find_all('tr')) and len(rows) > 1:
                # for each statistical row
                for row in rows[1:]:
                    # there is a row at the bottom of bench table that contains totals -- don't want that
                    if row.get('class') != 'total-row':
                        # gets all data cells in the row and make sure expected length matches
                        if (cells := row.find_all('td')) and len(cells) == 16:
                            # extracts subject data from shared data structure
                            if subject := bs_utils.extract_subject(cells[0], self.source.league, self.source.name):
                                # extracts the statistical data from the table
                                if box_score := bs_utils.extract_basketball_stats(cells[1:]):
                                    # update the shared box scores data structure
                                    self.update_box_scores(game_id, subject, box_score, stat_type='all')
