import asyncio

from bs4 import BeautifulSoup

from app.backend.data_collection.games import utils as gm_utils
from app.backend.data_collection.games.box_scores import utils as bs_utils


class FootballBoxScoreRetriever(bs_utils.BoxScoreRetriever):
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

    # TODO: DO PLAYER GAME LOGS GET UPDATED IN REAL TIME??
    async def _parse_box_score(self, html_content, game_id: str) -> None:
        # initializes a html parser
        soup = BeautifulSoup(html_content, 'html.parser')
        # extracts every Passing, Rushing, Receiving box score table for both teams --  3 in total
        divs = soup.find_all('div', {'class': 'stats-rows'})
        # for each table
        for div in divs:
            # get the table and the stat type for the table 'passing', 'rushing', etc.
            table, stat_type = div.find('table'), div.get('class').split('-')[0]
            # gets all rows in the box score table for starters or bench
            if (rows := table.find_all('tr')) and len(rows) > 1:
                # for each row
                for row in rows:
                    # get some data about extracting for the give stat type
                    extraction_info = bs_utils.FOOTBALL_TABLE_TYPE_MAP[stat_type]
                    # gets all data cells in the row and make sure expected length matches
                    if (cells := row.find_all('td')[1:]) and len(cells) == extraction_info['num_stats']:
                        # extracts subject data from shared data structure
                        if subject := bs_utils.extract_subject(cells[0], self.source.league, self.source.name):
                            # get the structured box score for the row
                            box_score = bs_utils.extract_football_stats(cells, extraction_info)
                            # update box scores
                            self.update_box_scores(game_id, subject, box_score, stat_type=stat_type)