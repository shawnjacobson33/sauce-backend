from bs4 import BeautifulSoup

from app.backend.data_collection.logistics.games import utils as gm_utils
from app.backend.data_collection.logistics.rosters import utils as rs_utils


class BasketballRosterRetriever(rs_utils.RosterRetriever):
    def __init__(self, source: gm_utils.GameSource):
        super().__init__(source)

    async def _parse_roster(self, html_content, team: dict) -> None:
        # initialize a parser
        soup = BeautifulSoup(html_content, 'html.parser')
        # finds the table that holds the roster
        if table := soup.find('table'):
            # get all the rows in the table
            if (rows := table.find_all('tr')) and (len(rows) > 1):
                # for each row excluding the headers
                for row in rows[1:]:
                    # get all the data cells from the row
                    if (cells := row.find_all('td')) and (len(cells) > 2):
                        # get the span element that holds the subject's name
                        if span_elem := cells[1].find('span', {'class': 'CellPlayerName--long'}):
                            # get the link element that holds the subject's name
                            if a_elem := span_elem.find('a'):
                                # create a subject object
                                self.update_subjects({
                                    'jersey_number': cells[0].text.strip(),
                                    'name': a_elem.text.strip(),
                                    'position': cells[2].text.strip(),
                                    'league': self.source.league_specific,
                                    'team_id': team['id'],
                                })

                self.log_team(team['abbr_name'])

