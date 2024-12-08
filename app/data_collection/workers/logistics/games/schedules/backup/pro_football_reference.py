from typing import Optional, Union

from bs4 import BeautifulSoup

from app import utils as dc_utils
from app import utils as gm_utils


def get_current_nfl_season():
    now = datetime.now()
    year = now.year
    month = now.month

    # Determine the season based on the month
    if month >= 10:  # October to December
        season = f"{year}"
    elif month <= 6:  # January to June
        season = f"{year - 1}"
    else:  # July to September (off-season)
        season = f"{year - 1}"

    return season


# TODO: Same as NHL
def convert_to_datetime(date_str, time_str) -> datetime:
    # Parse the date string
    date_obj = datetime.strptime(date_str, "%Y-%m-%d")
    # Parse the time string with AM/PM format
    time_obj = datetime.strptime(time_str, "%I:%M%p")
    # Combine the parsed date with the parsed time
    result = date_obj.replace(hour=time_obj.hour, minute=time_obj.minute, second=0, microsecond=0)
    # return the datetime object
    return result


def extract_data(tr_elem, attr_name: str) -> Optional[str]:
    # extracts any text from a data cell in the row for a given attribute name
    if td_elem := tr_elem.find('td', {'data-stat': attr_name}):
        return td_elem.text


def extract_game_time(n_days: int, row) -> Optional[datetime]:
    # get the game date and start time of the game if it exists
    if (game_date := extract_data(row, 'game_date')) and (start_time := extract_data(row, 'gametime')):
        # check if the game date is in the desired date range
        if game_date in gm_utils.get_date_range(n_days):
            # convert it to a comparable datetime object and then cast to a string
            return convert_to_datetime(game_date, start_time)


def extract_teams(source_name: str, league: str, row) -> Union[tuple[dict, dict], tuple[None, None]]:
    # gets both teams that played in the game
    if (winning_team := extract_data(row, 'winner')) and (losing_team := extract_data(row, 'loser')):
        # gets the '@' or None if winning team is home
        game_locator = extract_data(row, 'game_location')
        # returns in order of away_team, home_team depending on the locator
        away_team_name, home_team_name = (winning_team, losing_team) if game_locator == '@' else (losing_team, winning_team)
        # get the team id and team name from the database
        if away_team_data := dc_utils.get_team(source_name, league, away_team_name):
            # get the team id and team name from the database
            if home_team_data := dc_utils.get_team(source_name, league, home_team_name):
                # return the team id and team name
                return away_team_data, home_team_data

    return None, None


def extract_box_score_url(tr_elem) -> Optional[str]:
    # extracts the url pointing to the corresponding box score for this game
    if td_elem := tr_elem.find('td', {'data-stat': 'boxscore_word'}):
        if a_elem := td_elem.find('a'):
            return a_elem.get('href')


class NFLScheduleCollector(sc_utils.ScheduleRetriever):
    def __init__(self, source: gm_utils.GameSource):
        super().__init__(source)

    async def retrieve(self, n_days: int = 1) -> None:
        # get the url for pro-football-reference.com's nfl schedules
        url = lg_utils.get_url(self.source, 'schedule')
        # get the current season how it needs to be formatted
        current_season = get_current_nfl_season()
        # format the url with the dates
        formatted_url = url.format(current_season)
        # asynchronously request the data and call parse schedule
        await lg_utils.fetch(formatted_url, self._parse_schedule, n_days)

    async def _parse_schedule(self, html_content, n_days: int) -> None:
        # initializes a html parser
        soup = BeautifulSoup(html_content, 'html.parser')
        # extracts the table element that holds schedule data
        table = soup.find('table', {'id': 'games'})
        # extracts all rows except for the header row from the table
        rows = table.find_all('tr')[1:]
        # for each row
        for row in rows:
            # get the game time
            if game_time := extract_game_time(n_days, row):
                # extract the away and home team
                away_team, home_team = extract_teams(self.name, self.league, row)
                # if they exist in the database
                if away_team and home_team:
                    # adds the game and all of its extracted data to the shared data structure
                    self.update_games({
                        's_tstamp': str(datetime.now()),
                        "game_time": game_time,
                        "league": self.name,
                        "away_team": away_team,
                        "home_team": home_team,
                        "box_score_url": extract_box_score_url(row),
                    })
