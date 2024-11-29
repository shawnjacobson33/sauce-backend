from bs4 import BeautifulSoup

from app.backend.data_collection import utils as dc_utils
from app.backend.data_collection.logistics.games import utils as gm_utils
from app.backend.data_collection.logistics.rosters import utils as rs_utils


class IceHockeyRosterRetriever(rs_utils.RosterRetriever):
    def __init__(self, source: gm_utils.GameSource):
        super().__init__(source)

    async def _parse_roster(self, html_content, team: dict) -> None:
        # initialize a parser
        soup = BeautifulSoup(html_content, 'html.parser')
        # finds the table that holds the roster
        if tables := soup.find_all('table'):
            # for each of the tables Goalie and Non-Goalie
            for table in tables:
                # get all rows in the table
                if (rows := table.find_all('tr')) and (len(rows) > 1):
                    # for each row not including each header
                    for row in rows[1:]:
                        # get all the data cells in the row
                        if (cells := row.find_all('td')) and (len(cells) > 2):
                            # get the span element where the subject's name is found
                            if name_span_elem := cells[1].find('span', {'class': 'CellPlayerName--long'}):
                                # get the link element where the subject's name is found
                                if a_elem := name_span_elem.find('a'):
                                    # update the shared Subjects class with a subject object
                                    self.update_subjects({
                                        'jersey_number': cells[0].text.strip(),
                                        'name': a_elem.text.strip(),
                                        'position': cells[2].text.strip(),
                                        'league': self.league_spec,
                                        'team_id': team['id'],
                                    })

            self.log_team(team['abbr_name'])