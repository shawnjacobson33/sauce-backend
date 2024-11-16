import asyncio
from typing import Optional

from bs4 import BeautifulSoup

from app.backend.data_collection.games import utils as gm_utils
from app.backend.data_collection.games.box_scores import utils as bs_utils


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
        'Save Percentage': int(next(data)),
        'Time On Ice': extract_time_on_ice(data)
    }


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
        # get all the divs that contain the desired tables for both teams -- 2 for non-goalie, 2 for goalie
        if (divs := soup.find_all('div', {'class': 'gametracker-scrollable-table__content'})) and (len(divs) == 4):
            # for each div
            for div in divs:
                # get the table within that div
                table = div.find('table')
                # get all rows in the table
                if (rows := table.find_all('tr')) and len(rows) > 1:
                    # for each row not including the headers and totals
                    for row in rows[1:-1]:
                        # gets all data cells in the row and make sure expected length matches
                        if cells := row.find_all('td'):
                            # extracts subject data from shared data structure
                            if subject := bs_utils.extract_subject(cells[0], self.source.league, self.source.name):
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


