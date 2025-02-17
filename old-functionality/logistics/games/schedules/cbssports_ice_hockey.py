from datetime import datetime

from bs4 import BeautifulSoup

from app import Games
from app import utils as lg_utils
from app import utils as gm_utils
from app import utils as sc_utils


class IceHockeyScheduleRetriever(sc_utils.ScheduleRetriever):
    def __init__(self, source: gm_utils.GameSource):
        super().__init__(source)

    async def retrieve(self, n_days: int = 1) -> None:
        # Get the URL for the NBA schedule
        url = lg_utils.get_url(self.name, self.league_spec, 'schedule')
        # Use each date’s month to format URLs and fetch data
        for date in gm_utils.get_date_range(n_days, to_datetime=True):
            # format the url with the current season (2025) and month
            formatted_url = url.format(date.strftime("%Y%m%d"))
            # Asynchronously request the data and call parse schedule for each formatted URL
            await lg_utils.fetch(formatted_url, self._parse_schedule, date)

    async def _parse_schedule(self, html_content, date: datetime) -> None:
        # initializes a html parser
        soup = BeautifulSoup(html_content, 'html.parser')
        # extracts the table element that holds schedule data
        if table := soup.find('table'):
            # extracts all rows except for the header row from the table
            if rows := table.find_all('tr'):
                # for each row
                for row in rows[1:]:
                    # get the time and date of the game and check if it's in the right range of dates desired
                    game_time, box_score_url = sc_utils.extract_game_time_and_box_score_url(row, date)
                    # if the game time and box score url exist
                    if game_time and box_score_url:
                        # get the elements where team names lie
                        span_elems = row.find_all('span', {'class': 'TeamName'})
                        # make sure 2 teams exist
                        if len(span_elems) > 1:
                            # get the away team name and id if it exists
                            if away_team := sc_utils.extract_team(span_elems[0], self.name, self.league):
                                # get the home team name and id if it exists
                                if home_team := sc_utils.extract_team(span_elems[1], self.name, self.league):
                                    # checks if box scores are available for this game and updates accordingly
                                    if sc_utils.is_box_score_url_valid(box_score_url):
                                        # adds the game and all of its extracted data to the shared data structure
                                        Games.update_games({
                                            "league": self.league_spec,
                                            "game_time": game_time,
                                            "away_team": away_team,
                                            "home_team": home_team,
                                            "box_score_url": box_score_url
                                        })