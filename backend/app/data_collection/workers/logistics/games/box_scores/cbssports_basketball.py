from bs4 import BeautifulSoup

from backend.app.data_collection.workers.logistics.games import utils as gm_utils
from backend.app.data_collection.workers.logistics.games.box_scores import utils as bs_utils


class BasketballBoxScoreRetriever(bs_utils.BoxScoreRetriever):
    def __init__(self, source: gm_utils.GameSource):
        super().__init__(source)

    async def _parse_box_score(self, html_content, game: dict) -> None:
        # initializes a html parser
        soup = BeautifulSoup(html_content, 'html.parser')
        # get the divs that hold statistical data for starters and bench players
        if box_score_divs := soup.find_all('div', class_=['starters-stats', 'bench-stats']):
            # get the divs where the team name is located
            if team_name_divs := soup.find_all('div', {'class': 'team-name'}):
                # make sure the ratio of divs is correct
                if len(box_score_divs) == 2*len(team_name_divs):
                    # for each team
                    for team_name_div in team_name_divs:
                        # for each box score type
                        for box_score_div in box_score_divs:
                            # get team data from db data and div
                            if team := bs_utils.extract_team(team_name_div, game):
                                # get the table div that holds the statistical data
                                if table_div := box_score_div.find('div', {'class': 'stats-viewable-area'}):
                                    # extracts every starter and bench players box score table for both teams --  4 in total
                                    if table := table_div.find('table', {'class': 'stats-table'}):
                                        # gets all rows in the box score table for starters or bench
                                        if (rows := table.find_all('tr')) and len(rows) > 1:
                                            # for each statistical row...the starters box score table doesn't have a totals row
                                            for row in [row for row in rows if row.get('class')[0] not in {'header-row', 'total-row'}]:
                                                # gets all data cells in the row and make sure expected length matches
                                                if cells := row.find_all('td'):
                                                    # don't want extra formatting cells
                                                    cells = [cell for cell in cells if 'for-mobile' not in cell.get('class')]
                                                    # extracts subject data from shared data structure
                                                    if subject := bs_utils.extract_subject(cells[0], self.league_spec,
                                                                                           self.name, team=team):
                                                        # TODO: need to think a bit more about subject name standardization
                                                        # extracts the statistical data from the table
                                                        if box_score := bs_utils.extract_basketball_stats(cells[1:], self.league_spec):
                                                            # update the shared box scores data structure
                                                            self.update_box_scores(game['id'], subject, box_score, stat_type='all')
