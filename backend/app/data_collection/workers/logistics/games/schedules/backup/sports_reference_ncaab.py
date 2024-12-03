import asyncio
from collections import deque
from typing import Optional, Union

from bs4 import BeautifulSoup

from backend.app.data_collection.workers import utils as dc_utils
from backend.app.data_collection.workers.logistics.games import utils as gm_utils


# TODO: Same as NBA
def get_game_time(date: datetime, time_str: str) -> datetime:
    # Append "AM" or "PM" to the time string based on "a" or "p" suffix
    if time_str[-1].lower() == 'p':
        time_str = time_str[:-1] + "PM"
    elif time_str[-1].lower() == 'a':
        time_str = time_str[:-1] + "AM"

    # Parse the updated time string
    time_obj = datetime.strptime(time_str, "%I:%M%p")
    # Combine today's date with the parsed time
    combined_datetime = date.replace(hour=time_obj.hour, minute=time_obj.minute, second=0, microsecond=0)
    # return the combined date
    return combined_datetime


def extract_away_team(source_name: str, league: str, rows) -> Optional[dict]:
    # extracts any text from a data cell in the row for a given attribute name
    if td_elem := rows[0].find('td'):
        # if the link element exists
        if a_elem := td_elem.find('a'):
            # get the away team name
            away_team_name = a_elem.text
            # get the team id and team name from the database
            if team_data := dc_utils.get_team(source_name, league, away_team_name):
                # return the team id and team name
                return team_data


def extract_home_team_and_game_time(source_name: str, league: str, rows, date: datetime) -> Union[tuple[dict, datetime], tuple[None, None]]:
    # if rows as more than 1 element
    if len(rows) > 1:
        # get all the cells from the second row
        if (td_elems := rows[1].find_all('td')) and (len(td_elems) > 2):
            # get the link element from the first cell and the third cell
            if (a_elem := td_elems[0].find('a')) and (start_time := td_elems[2]):
                # only extract if the game hasn't started yet i.e. the text has a time
                if start_time.text[0].isdigit():
                    # get the home team name from the link's inner html
                    home_team_name = a_elem.text
                    # get the team id and team name from the database
                    if team_data := dc_utils.get_team(source_name, league, home_team_name):
                        # extract and convert the game time to a date and then cast to a string
                        game_time = get_game_time(date, start_time.text.strip())
                        # return the home team and game time
                        return team_data, game_time

    return None, None


def extract_league(rows) -> Optional[str]:
    if len(rows) > 2:
        if td_elem := rows[2].find('td'):
            return 'NCAAM' if 'Men' in td_elem.text else 'NCAAW'


class NCAABScheduleCollector(sc_utils.ScheduleRetriever):
    def __init__(self, source: gm_utils.GameSource):
        super().__init__(source)

    async def retrieve(self, n_days: int = 1) -> None:
        # generate a range of dates predicated upon n_days param
        date_list = gm_utils.get_date_range(n_days, to_datetime=True)
        # get the url for pro-football-reference.com's nfl schedules
        url = lg_utils.get_url(self.source, 'schedule')
        # initialize a list of to store requests to make
        tasks = list()
        # for each date desired in the date list range
        for date in date_list:
            # get the current season how it needs to be formatted
            c_month, c_day, c_year = date.month, date.day, date.year
            # format the url with the dates
            formatted_url = url.format(c_month, c_day, c_year)
            # asynchronously request the data and call parse schedule
            tasks.append(lg_utils.fetch(formatted_url, self._parse_games, date))

        # start asynchronously making requests
        await asyncio.gather(*tasks)

    async def _parse_games(self, html_content, date: datetime) -> None:
        # initializes a html parser
        soup = BeautifulSoup(html_content, 'html.parser')
        # extracts the table element that holds schedule data
        tables = soup.find_all('table', {'class': 'teams'})
        # for each table
        for table in tables:
            # get all the rows
            if rows := table.find_all('tr'):
                # extract the away team
                if away_team := extract_away_team(self.name, self.league, rows):
                    # extract both home team and game time, located on the same row
                    home_team, game_time = extract_home_team_and_game_time(self.name, self.league, rows, date)
                    # if both exist
                    if home_team and game_time:
                        # extract the league (NCAAM or NCAAW)
                        if league := extract_league(rows):
                            # adds the game and all of its extracted data to the shared data structure
                            self.update_games({
                                's_tstamp': str(datetime.now()),
                                "game_time": game_time,
                                "league": league,
                                "away_team": away_team,
                                "home_team": home_team
                            })
