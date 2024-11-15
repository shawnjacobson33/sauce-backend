from datetime import datetime
from typing import Optional

from bs4 import BeautifulSoup

from app.backend.data_collection import utils as dc_utils
from app.backend.data_collection.games import utils as gm_utils


def is_date_valid(date_str: str, n_days: int) -> str:
    # generate a range of dates predicated upon n_days param
    date_list = gm_utils.get_date_range(n_days)
    # Parse the input date string into a datetime object
    input_date = datetime.strptime(date_str.strip(), '%A, %B %d, %Y').strftime("%Y-%m-%d")
    # check if the date is in the desired range
    if input_date in date_list:
        # return the formatted date
        return input_date


def clean_team(source_name: str, league: str, team_name: str) -> Optional[dict]:
    # Find the index of the first letter
    first_letter_index = next((i for i, char in enumerate(team_name) if char.isalpha()), -1)
    # return the cleaned team name
    cleaned_team_name = team_name[first_letter_index:]
    # get the team id and team name from the database
    if team_data := dc_utils.get_team_id(source_name, league, cleaned_team_name):
        # return the team id and team name
        return team_data


def get_full_game_date(date_str: str, time_str: str) -> datetime:
    # Combine today's date with the time string
    return datetime.strptime(f"{date_str} {time_str}", "%Y-%m-%d %I:%M %p")


def extract_game_time(date_str: str, cell) -> Optional[datetime]:
    if a_elem := cell.find('a'):
        return get_full_game_date(date_str, a_elem.text)


class NCAAFScheduleCollector(sc_utils.ScheduleRetriever):
    def __init__(self, source: gm_utils.GameSource):
        super().__init__(source)

    async def retrieve(self, n_days: int = 1) -> None:
        # get the url for cbssports.com's ncaaf schedules
        url = gm_utils.get_url(self.source.name, 'schedule')
        # asynchronously request the data and call parse schedule
        await gm_utils.fetch(url, self._parse_games, n_days)

    async def _parse_games(self, html_content, n_days: int) -> None:
        # initializes a html parser
        soup = BeautifulSoup(html_content, 'html.parser')
        # get all the divs that contain all rows
        div_elems = soup.find_all('div', {'class': 'TableBaseWrapper'})
        # for each div
        for div_elem in div_elems:
            # check if the h4 element contains the correct date for desired date range
            if (h4_elem := div_elem.find('h4')) and (game_date := is_date_valid(h4_elem.text, n_days)):
                # get all the rows (games) for today
                if (rows := div_elem.find_all('tr')) and (len(rows) > 1):
                    # for each row not including the header
                    for row in rows[1:]:
                        # get all the cells for the game
                        if (cells := row.find_all('td')) and (len(cells) > 2):
                            # extract the away team and home team if they exist
                            if (away_team_name := cells[0].text) and (home_team_name := cells[1].text):
                                # get the away team data from db
                                if away_team := clean_team(self.source.name, self.source.league, away_team_name):
                                    # get the home team data from db
                                    if home_team := clean_team(self.source.name, self.source.league,
                                                               home_team_name):
                                        # extract the game time
                                        if game_time := extract_game_time(game_date, cells[2]):
                                            # adds the game and all of its extracted data to the shared data structure
                                            self.update_games({
                                                'time_processed': datetime.now(),
                                                "game_time": game_time,
                                                "league": self.source.league,
                                                "away_team": away_team,
                                                "home_team": home_team
                                            })
