from bs4 import BeautifulSoup

from app.backend.data_collection.logistics.games import utils as gm_utils
from app.backend.data_collection.logistics.games.box_scores import utils as bs_utils


class FootballBoxScoreRetriever(bs_utils.BoxScoreRetriever):
    def __init__(self, source: gm_utils.GameSource):
        super().__init__(source)

    # TODO: DO PLAYER GAME LOGS GET UPDATED IN REAL TIME??
    async def _parse_box_score(self, html_content, game: dict) -> None:
        # initializes a html parser
        soup = BeautifulSoup(html_content, 'html.parser')
        # get the player stats div
        if player_stats_div := soup.find('div', {'class': 'player-stats-container'}):
            # get the divs where the team name is located
            if team_name_divs := player_stats_div.find_all('div', {'class': 'team-name'}):
                # extracts every Passing, Rushing, Receiving box score table for both teams --  3 in total
                if box_score_divs := player_stats_div.find_all('div', {'class': 'stats-ctr-container'}):
                    # for both team name div and team specific box score
                    for team_name_div, team_box_score_div in zip(team_name_divs, box_score_divs):
                        # we don't want any 'return' box scores
                        for box_score_div in [div for div in team_box_score_div.contents if div != '\n'][:-2]:
                            # get team data from db data and div
                            if team := bs_utils.extract_team(team_name_div, game):
                                # get the stats div that holds actual statistical table
                                if stats_div := box_score_div.find('div', {'class': 'stats-rows'}):
                                    # get the table and the stat type for the table 'passing', 'rushing', etc.
                                    if (table := stats_div.find('table')) and (div_class := box_score_div.get('class')):
                                        # get the stat type of the box score table
                                        stat_type = div_class[0].split('-')[0]
                                        # gets all rows in the box score table for starters or bench
                                        if rows := [row for row in table.contents if row != '\n']:
                                            # for each valid row
                                            for row in rows:
                                                # get some data about extracting for the give stat type
                                                if extraction_info := bs_utils.FOOTBALL_TABLE_TYPE_MAP.get(stat_type):
                                                    # gets all data cells in the row and make sure expected length matches, includes team and name cell
                                                    if cells := [cell for cell in row.contents if cell != '\n']:
                                                        # dont need team cell
                                                        cells = cells[1:]
                                                        # extracts subject data from shared data structure
                                                        if subject := bs_utils.extract_subject(cells[0], self.league,
                                                                                               self.name, team=team):
                                                            # get the structured box score for the row
                                                            box_score = bs_utils.extract_football_stats(cells[1:], extraction_info)
                                                            # update box scores
                                                            self.update_box_scores(game['id'], subject, box_score, stat_type=stat_type)