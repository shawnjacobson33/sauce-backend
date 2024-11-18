import asyncio

from bs4 import BeautifulSoup

from app.backend.data_collection.logistics import utils as lg_utils
from app.backend.data_collection.logistics.games import utils as gm_utils
from app.backend.data_collection.logistics.games.box_scores import utils as bs_utils


class BasketballBoxScoreRetriever(bs_utils.BoxScoreRetriever):
    def __init__(self, source: gm_utils.GameSource):
        super().__init__(source)

    async def retrieve(self) -> None:
        # initialize a list of requests to make
        tasks = list()
        # get the games to retrieve box scores from if there are any
        if games_to_retrieve := self.get_games_to_retrieve():
            # for every game
            for game_id, game_data in games_to_retrieve.items():
                # Get the URL for the NBA schedule
                url = lg_utils.get_url(self.source, 'box_scores')
                # format the url with the unique url piece stored in the game dictionary
                formatted_url = url.format(game_data['box_score_url'])
                # Asynchronously request the data and call parse schedule for each formatted URL
                tasks.append(lg_utils.fetch(formatted_url, self._parse_box_score, game_id))

            # gather all requests asynchronously
            await asyncio.gather(*tasks)

    async def _parse_box_score(self, html_content, game_id: str) -> None:
        # initializes a html parser
        soup = BeautifulSoup(html_content, 'html.parser')
        # get the divs that hold statistical data for starters and bench players
        if divs := soup.find_all('div', class_=['starters-stats', 'bench-stats']):
            # for each div
            for div in divs:
                # get the table div that holds the statistical data
                if table_div := div.find('div', {'class': 'stats-viewable-area'}):
                    # extracts every starter and bench players box score table for both teams --  4 in total
                    if table := table_div.find('table', {'class': 'stats-table'}):
                        # gets all rows in the box score table for starters or bench
                        if (rows := table.find_all('tr')) and len(rows) > 1:
                            # for each statistical row
                            for row in rows[1:-1]:
                                # gets all data cells in the row and make sure expected length matches
                                if (cells := row.find_all('td')) and len(cells) == 16:
                                    # extracts subject data from shared data structure
                                    if subject := bs_utils.extract_subject(cells[0], self.source.league, self.source.name):
                                        # extracts the statistical data from the table
                                        if box_score := bs_utils.extract_basketball_stats(cells[1:]):
                                            # update the shared box scores data structure
                                            self.update_box_scores(game_id, subject, box_score, stat_type='all')
