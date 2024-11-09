import asyncio
import pprint
from datetime import datetime
from typing import Optional, Union, Any

from bs4 import BeautifulSoup

from app.data_collection.games import utils as gm_utils


# TODO: Same as NBA
def convert_to_datetime_today(time_str):
    # Get today's date
    today = datetime.now()

    # Append "AM" or "PM" to the time string based on "a" or "p" suffix
    if time_str[-1].lower() == 'p':
        time_str = time_str[:-1] + "PM"
    elif time_str[-1].lower() == 'a':
        time_str = time_str[:-1] + "AM"

    # Parse the updated time string
    time_obj = datetime.strptime(time_str, "%I:%M%p")

    # Combine today's date with the parsed time
    combined_datetime = today.replace(hour=time_obj.hour, minute=time_obj.minute, second=0, microsecond=0)

    return combined_datetime


def extract_away_team(rows) -> Optional[str]:
    # extracts any text from a data cell in the row for a given attribute name
    if td_elem := rows[0].find('td'):
        if a_elem := td_elem.find('a'):
            return a_elem.text


def extract_home_team_and_game_time(rows) -> Union[tuple[Any, str], tuple[None, None]]:
    # if rows as more than 1 element
    if len(rows) > 1:
        # get all the cells from the second row
        if (td_elems := rows[1].find_all('td')) and (len(td_elems) > 2):
            # get the link element from the first cell and the third cell
            if (a_elem := td_elems[0].find('a')) and (start_time := td_elems[2]):
                # only extract if the game hasn't started yet i.e. the text has a time
                if start_time.text[0].isdigit():
                    # extract and convert the game time to a date and then cast to a string
                    game_time = str(convert_to_datetime_today(start_time.text.strip()))
                    # return the home team and game time
                    return a_elem.text, game_time

    return None, None


def extract_league(rows) -> Optional[str]:
    if len(rows) > 2:
        if td_elem := rows[2].find('td'):
            return 'NCAAM' if 'Men' in td_elem.text else 'NCAAW'


class NCAABScheduleCollector(gm_utils.ScheduleCollector):
    def __init__(self, source_info: gm_utils.Source):
        super().__init__(source_info)

    async def collect(self) -> None:
        # get the url for pro-football-reference.com's nfl schedules
        url = gm_utils.get_url(self.source_info.name, self.source_info.league, 'schedule')
        # get the current season how it needs to be formatted
        c_date = datetime.now()
        c_month, c_day, c_year = c_date.month, c_date.day, c_date.year
        # format the url with the dates
        formatted_url = url.format(c_month, c_day, c_year)
        # asynchronously request the data and call parse schedule
        await gm_utils.fetch(formatted_url, self._parse_games)

    async def _parse_games(self, html_content) -> None:
        # initializes a html parser
        soup = BeautifulSoup(html_content, 'html.parser')
        # extracts the table element that holds schedule data
        tables = soup.find_all('table', {'class': 'teams'})
        # for each table
        for table in tables:
            # get all the rows
            if rows := table.find_all('tr'):
                # extract the away team
                if away_team := extract_away_team(rows):
                    # extract both home team and game time, located on the same row
                    home_team, game_time = extract_home_team_and_game_time(rows)
                    # if both exist
                    if home_team and game_time:
                        # extract the league (NCAAM or NCAAW)
                        if league := extract_league(rows):
                            # adds the game and all of its extracted data to the shared data structure
                            self.update_games({
                                'time_processed': str(datetime.now()),
                                "game_time": game_time,
                                "league": league,
                                "away_team": away_team,
                                "home_team": home_team
                            })


async def main():
    from app.data_collection.games.utils.shared_data import Games
    source = gm_utils.Source('Reference', 'NCAAB')
    await NCAABScheduleCollector(source).collect()
    pprint.pprint(Games.get_games())

if __name__ == '__main__':
    asyncio.run(main())
