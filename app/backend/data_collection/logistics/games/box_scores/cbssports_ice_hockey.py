from typing import Optional

from bs4 import BeautifulSoup

from app.backend.data_collection.logistics.games import utils as gm_utils
from app.backend.data_collection.logistics.games.box_scores import utils as bs_utils


def extract_time_on_ice(data) -> Optional[float]:
    # get the minutes and seconds separately
    toi_comps = next(data).split(':')
    # if both exist
    if len(toi_comps) > 1:
        # get time in minutes and fraction of seconds from a minute...13:15 -> 13.25
        return round(int(toi_comps[0]) + float(toi_comps[1]) / 60, 2)


def extract_non_goalie_stats(cells) -> dict[str, int]:
    # get all the data in text form
    data = (cell_comp for cell in cells
            for cell_comp in cell.text.split('/'))
    return {
        'Goals': int(next(data)),
        'Assists': int(next(data)),
        'Plus Minus': int(next(data)),
        'Shots On Goal': int(next(data)),
        'Faceoffs Won': int(next(data)),
        'Faceoffs Lost': int(next(data)),
        'Penalty Minutes': int(next(data)),
        'Time On Ice': extract_time_on_ice(data),
        'Hits': int(next(data))
    }


def extract_goalie_stats(cells) -> dict[str, int]:
    # get all the data in text form
    data = (cell.text for cell in cells)
    return {
        'Shots Against': int(next(data)),
        'Goals Against': int(next(data)),
        'Saves': int(next(data)),
        'Save Percentage': float(next(data)),
        'Time On Ice': extract_time_on_ice(data)
    }


class IceHockeyBoxScoreRetriever(bs_utils.BoxScoreRetriever):
    def __init__(self, source: gm_utils.GameSource):
        super().__init__(source)

    async def _parse_box_score(self, html_content, game_id: str) -> None:
        # initializes a html parser
        soup = BeautifulSoup(html_content, 'html.parser')
        # get the div containing all box scores tables
        if div := soup.find('div', {'class': 'gametracker-table--boxscore-player-stats'}):
            # get all the divs containing box score tables
            if box_score_divs := div.find_all('div', {'class': 'gametracker-app__table-row gametracker-app__table-row--split'}):
                # for each div
                for box_score_div in box_score_divs:
                    # get all divs inside box score type div
                    if team_spec_divs := box_score_div.contents:
                        # for each individual team's box score type div
                        for team_spec_div in team_spec_divs:
                            # get the table body that stores the box score data
                            if tbody := team_spec_div.find('tbody', {'class': 'gametracker-table__tbody'}):
                                # get all rows in the table
                                if rows := tbody.find_all('tr'):
                                    # for each row not including the totals
                                    for row in [row for row in rows if 'gametracker-table__tr' in row.get('class')]:
                                        # gets all data cells in the row and make sure expected length matches
                                        if cells := row.find_all('td'):
                                            # extracts subject data from shared data structure
                                            if subject := bs_utils.extract_subject(cells[0], self.league, self.name):
                                                # get everything but the player name
                                                stats = cells[1:]
                                                # this means that it is a non-goalie box score
                                                if len(stats) == 8:
                                                    # get the non goalie box score data
                                                    if non_goalie_box_score := extract_non_goalie_stats(stats):
                                                        # update the shared box score data class
                                                        self.update_box_scores(game_id, subject, non_goalie_box_score, stat_type='all')
                                                # this means it is a goalie box score
                                                elif len(stats) == 5:
                                                    # get the goalie box score
                                                    if goalie_box_score := extract_goalie_stats(stats):
                                                        # update the shared box score data class
                                                        self.update_box_scores(game_id, subject, goalie_box_score, stat_type='all')


