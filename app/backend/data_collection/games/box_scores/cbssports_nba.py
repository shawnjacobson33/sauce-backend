import asyncio
from typing import Optional

from bs4 import BeautifulSoup

from app.backend.data_collection import utils as dc_utils
from app.backend.data_collection.games import utils as gm_utils
from app.backend.data_collection.bookmakers import utils as bkm_utils
from app.backend.data_collection.games.box_scores import utils as bs_utils


def extract_subject(cell, league: str, source_name: str) -> Optional[dict]:
    # get the link element
    if a_elem := cell.find('a'):
        # extract the player's name from the url link
        player_name = ' '.join(a_elem.get('href').split('/')[-2].split('-')).title()
        # get subject data from the shared data structure
        subject = bkm_utils.get_subject_id(source_name, league, player_name)
        # return the subject data
        return subject


def extract_stats(cells) -> Optional[dict]:
    # get all data from each cell element
    data = [cell.text for cell in cells]
    # make sure this player actually played
    if data[0] != '-':
        # extract and cast all relevant data
        statistical_data_structured = {
            'Points': int(data[0]),
            'Rebounds': int(data[1]),
            'Assists': int(data[2]),
            'Field Goals Made': int(data[3].split('/')[0]),
            'Field Goals Attempted': int(data[3].split('/')[1]),
            '3-Pointers Made': int(data[4].split('/')[0]),
            '3-Pointers Attempted': int(data[4].split('/')[1]),
            'Free Throws Made': int(data[5].split('/')[0]),
            'Free Throws Attempted': int(data[5].split('/')[1]),
            'Personal Fouls': int(data[6]),
            'Minutes Played': int(data[7]),
            'Steals': int(data[8]),
            'Blocks': int(data[9]),
            'Turnovers': int(data[10]),
            'Plus Minus': data[11],
            'Fantasy Points': int(data[12])
        }
        # return the statistical data
        return statistical_data_structured


class NBABoxScoreRetriever(bs_utils.BoxScoreRetriever):
    def __init__(self, source: gm_utils.GameSource):
        super().__init__(source)

    async def retrieve(self) -> None:
        # from the Games shared data structure get the games that have betting lines associated with them
        games_to_retrieve: dict = dc_utils.Games.get_active_games(self.source.league)
        # initialize a list of requests to make
        tasks = list()
        # for every game
        for game_id, game in games_to_retrieve.items():
            # Get the URL for the NBA schedule
            url = gm_utils.get_url(self.source.name, 'box_scores')
            # format the url with the unique url piece stored in the game dictionary
            formatted_url = url.format(game['box_score_url'])
            # Asynchronously request the data and call parse schedule for each formatted URL
            tasks.append(gm_utils.fetch(formatted_url, self._parse_box_score, game_id))

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
                            if subject := extract_subject(cells[0], self.source.league, self.source.name):
                                # extracts the statistical data from the table
                                if stats := extract_stats(cells[1:]):
                                    # stores and structures all box score data
                                    box_score = {
                                        'subject': subject['name'],
                                        **{stats}
                                    }
                                    # update the shared box scores data structure
                                    self.update_box_scores(game_id, subject['id'], box_score)
